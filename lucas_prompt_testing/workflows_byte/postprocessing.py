
import os
import pandas as pd 
from typing import Literal, Optional
from google.cloud import bigquery
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

from src.news_package.modelling.features_utils import get_company_names_from_metadata
from src.news_package.processing.filtering import remove_stock_picks, remove_short_articles, heuristic_filters
from src.news_package.processing.fuzzy_deduplication import remove_historical_duplicates, remove_internal_duplicates
from src.news_package.modelling.generate_features import generate_features
from src.news_package.LLM_articles_classifier.article_classifier import ArticleClassifier
from src.news_package.LLM_articles_classifier.article_relevance_prompts import NEWS_ARTICLE_CLASSIFIER_PROMPT


class BytePostProcessing(object):
    def __init__(self,
                 sector: Literal["Index Bank", "Insurance1000", "Index Insurance", "Payments", "Index1000", "Other"],
                 start_date=None, # datetime for when to start article search from, default is X days ago, where X = number_of_days. - example input: datetime(2025, 7, 1, 0, 0)
                 end_date=None, # datetime cutoff for article search, default is current timestamp today - example input: datetime(2025, 7, 15, 23, 59)
                 number_of_days: Optional[int] = 7, # see start_date #TODO: change this desc to publication date bc of query in get_articles_df()
                 skip_hist_deduper=False,
                 test_run=False,
                 qa_data=False,
                 ):
        
        # constants
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT") # set gcloud project inside the .env file TODO: change to evident-data-dev eventually 
        if not self.project_id:
            raise EnvironmentError("GOOGLE_CLOUD_PROJECT is not set in the environment or .env file.")
        self.sector = sector

        if not number_of_days:
            self.number_of_days = 3
        else:
            self.number_of_days = number_of_days

        if not end_date:
            self.end_date = datetime.now()
        else:
            self.end_date = end_date
        
        if not start_date:
            self.start_date = self.end_date - timedelta(days=self.number_of_days)
        else:
            self.start_date = start_date
        
        self.skip_hist_deduper = skip_hist_deduper
        self.test_run = test_run
        self.qa_data = qa_data
        self.bq_client = bigquery.Client(project=self.project_id)
        self.company_names = get_company_names_from_metadata(sector=self.sector, client=self.bq_client)


    def get_articles_df(self):
        """
        Get articles from raw table in bigquery and create pandas df.
        Note this query filters the articles by published date and media source, as well as performing deduplication based on url and article title.
        """
        #TODO: is pub date > start date cutting off articles???
        clean_sector = {"Index Bank": "banking",
                            "Index Insurance": "insurance",
                            "Payments": "payments"}
        
        source_tbl = (f"{self.project_id}.raw_newsapi.{clean_sector[self.sector]}_articles"
                        if not self.test_run else 
                        f"{self.project_id}.temporary.byte_{clean_sector[self.sector]}_newsapi_articles"
        )

        # SQL query summary:
            # 1. Extracts and flattens relevant fields from nested article JSON (source info, metadata, timestamps).
            # 2. Filters out articles marked as duplicates and restricts to a specific date range.
            # 3. Joins with a reference table of top-tier sources and assigns a custom "source_rank_infill".
            # 4. Removes duplicate articles by URL, keeping the one with the best (lowest) source rank.
            # 5. Removes duplicate articles by title using the same ranking logic.
            # 6. Final output keeps only articles from the top-tier or highest-ranked source within each duplicate group.

        query = f"""
            with base AS (
            SELECT

            -- Root-level fields
            JSON_EXTRACT_SCALAR(a.article_json, '$.temp_id') AS id,
            JSON_EXTRACT_SCALAR(a.article_json, '$.article_id') AS article_id,
            JSON_EXTRACT_SCALAR(a.article_json, '$.company_id') AS company_id,
            JSON_EXTRACT_SCALAR(a.article_json, '$.sector') AS sector,
            JSON_EXTRACT_SCALAR(a.article_json, '$.lang') AS lang,
            JSON_EXTRACT_SCALAR(a.article_json, '$.dataType') AS dataType,
            JSON_EXTRACT_SCALAR(a.article_json, '$.url') AS url,
            JSON_EXTRACT_SCALAR(a.article_json, '$.title') AS title,
            JSON_EXTRACT_SCALAR(a.article_json, '$.body') AS body,

            -- Source object
            JSON_EXTRACT_SCALAR(a.article_json, '$.source.uri') AS source_uri,
            JSON_EXTRACT_SCALAR(a.article_json, '$.source.dataType') AS source_dataType,
            JSON_EXTRACT_SCALAR(a.article_json, '$.source.title') AS source_name,
            JSON_EXTRACT_SCALAR(a.article_json, '$.source.description') AS source_description,
            JSON_EXTRACT_SCALAR(a.article_json, '$.source.location.country.label.eng') AS source_country,
            JSON_EXTRACT_SCALAR(a.article_json, '$.source.location.label.eng') AS source_location,
            CAST(JSON_EXTRACT_SCALAR(a.article_json, '$.source.ranking.importanceRank') AS INT64) AS source_importance_rank,

            -- Timestamps
            JSON_EXTRACT_SCALAR(a.article_json, '$.date') AS date_found_by_newsapi,
            JSON_EXTRACT_SCALAR(a.article_json, '$.dateTime') AS dateTime_found_by_newsapi,
            JSON_EXTRACT_SCALAR(a.article_json, '$.dateTimePub') AS dateTimePub,
            JSON_EXTRACT_SCALAR(a.article_json, '$.run_datetime') as ingestion_run_datetime,

            FROM {source_tbl} a
            WHERE CAST(JSON_EXTRACT_SCALAR(a.article_json, '$.isDuplicate') AS BOOL) IS FALSE
            AND DATE(JSON_EXTRACT_SCALAR(a.article_json, '$.dateTimePub')) >= DATE('{self.start_date}')
            AND DATE(JSON_EXTRACT_SCALAR(a.article_json, '$.dateTimePub')) <= DATE_ADD(DATE('{self.start_date}'), INTERVAL {self.number_of_days} DAY)
            )

            , refresh_source_importance_ranking AS (
            SELECT
            *,
            CASE WHEN is_top_tier=TRUE THEN 1
                 ELSE source_importance_rank+1
                 END AS source_rank_infill 
            FROM (SELECT 
                        bb.*,
                        cc.is_top_tier 
                        FROM base bb
                        LEFT JOIN `evident-data.staging.byte_news_outlets_sourceuris` cc
                        ON bb.source_uri = cc.eventregistry_sourceuri
                        WHERE is_top_tier is TRUE
                    )
            )

            , remove_duplicate_urls as (
            SELECT 
            *
            FROM (  SELECT *,
                            ROW_NUMBER() OVER(PARTITION BY url ORDER BY source_rank_infill ASC, source_importance_rank ASC) as rn_url, #NOTE source_importance_rank LOW value = higher importance)
                    FROM refresh_source_importance_ranking
                    ) 
            WHERE rn_url = 1
            )

            , remove_duplicate_titles as (
            SELECT 
            *
            FROM (SELECT *,
                        ROW_NUMBER() OVER(PARTITION BY title ORDER BY source_rank_infill ASC, source_importance_rank ASC) as rn_title
                        FROM remove_duplicate_urls
                    ) 
            WHERE rn_title = 1
            )

            , final AS (
            SELECT 
            * except(rn_url, 
                      rn_title
                       )

            FROM remove_duplicate_titles
            WHERE source_rank_infill = 1
            )

            SELECT
            *
            FROM final
        """

        query_job = self.bq_client.query(query)
        df = query_job.to_dataframe()

        if df.empty:
            print("no articles in df")
        else:
            print("articles df created")
            print(df.head())
        
        return df
    

    def trigger_workflow(self):

        ### Create articles df, filter unwanted content, and remove duplicates ###
        articles = self.get_articles_df()
        articles['run_datetime'] = datetime.now()
        print(f"Initial Number Of Articles: {articles.shape[0]}")

        qa_filtered_articles = []

        articles, short_articles = remove_short_articles(articles, min_words=75)
        print(f"After Removing Short Articles: {articles.shape[0]}")

        articles, stock_picks = remove_stock_picks(articles)
        print(f"After Removing Stock Pick Articles: {articles.shape[0]}")

        articles, internal_duplicates = remove_internal_duplicates(articles, 
                                                                   embedding_model='all-MiniLM-L6-v2')
        print(f"After Removing Internal Duplicates: {articles.shape[0]}")

        if self.skip_hist_deduper:
            print("skipped historical de-duplication step...")
        else: 
            articles, hist_duplicates = remove_historical_duplicates(articles,
                                                                     client=self.bq_client, 
                                                                     end_date=self.end_date,
                                                                     project_id=self.project_id,
                                                                     sector=self.sector,
                                                                     test_run=self.test_run)
            print(f"After Removing Historical Duplicates: {articles.shape[0]}")
        
        # generate features
        articles = generate_features(articles, company_names=self.company_names)
        
        articles[['keep_or_remove', 'remove_reason']] = articles.apply(lambda row: pd.Series(heuristic_filters(row)), axis=1)
        removed_by_features = articles[articles.keep_or_remove != "Keep"].copy()
        articles = articles[articles.keep_or_remove == "Keep"]
        print(f"After filtering the articles' features using heuristics: {len(articles)}")

        # pass the filtered articles to an LLM for classification
        filtered_articles = articles.copy()

        news_article_classifier = ArticleClassifier(instructions_prompt=NEWS_ARTICLE_CLASSIFIER_PROMPT.format(
                                company_list=self.company_names),
                                project_id=self.project_id)

        classified_articles = news_article_classifier.classify(
            text_input=filtered_articles,
            text_column="body",
            identifiable_column="id"
        )
        classified_articles = pd.merge(filtered_articles,
                                       classified_articles.drop(columns=["body"]),
                                       on="id")
        relevant_articles = classified_articles[classified_articles.label == "relevant"]
        non_relevant_articles = classified_articles[classified_articles.label != "relevant"].copy()
        print(f"Articles LLM classifier classed as relevant: {len(relevant_articles)}")

        ### Upload to BigQuery ###
        relevant_articles_slim = relevant_articles[[
            "id",
            "company_id",
            "sector",
            "url",
            "title",
            "body",
            "source_name",
            "pillar",
            "dateTimePub",
            "run_datetime"
        ]].copy()

        # Ensure proper dtypes before upload
        relevant_articles_slim["company_id"] = pd.to_numeric(relevant_articles_slim["company_id"], errors="coerce")  # Converts to float64 or int
        relevant_articles_slim["dateTimePub"] = pd.to_datetime(relevant_articles_slim["dateTimePub"], errors="coerce")
        relevant_articles_slim["run_datetime"] = pd.to_datetime(relevant_articles_slim["run_datetime"], errors="coerce")

        schema = [
                    bigquery.SchemaField("id", "STRING"),
                    bigquery.SchemaField("company_id", "INTEGER"),
                    bigquery.SchemaField("sector", "STRING"),
                    bigquery.SchemaField("url", "STRING"),
                    bigquery.SchemaField("title", "STRING"),
                    bigquery.SchemaField("body", "STRING"),
                    bigquery.SchemaField("source_name", "STRING"),
                    bigquery.SchemaField("pillar", "STRING"),
                    bigquery.SchemaField("dateTimePub", "TIMESTAMP"),
                    bigquery.SchemaField("run_datetime", "TIMESTAMP"),
                ]
        
        clean_sector = {"Index Bank": "banking",
                            "Index Insurance": "insurance",
                            "Payments": "payments"}
        
        temp_tbl = (f"{self.project_id}.curated_byte.{clean_sector[self.sector]}_articles_temp"
                    if not self.test_run else 
                    f"{self.project_id}.temporary.byte_{clean_sector[self.sector]}_articles_temp"
        )
        job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition="WRITE_TRUNCATE"
        )

        job = self.bq_client.load_table_from_dataframe(relevant_articles_slim, temp_tbl, job_config=job_config)
        job.result()
        print(f"Uploaded {len(relevant_articles)} rows to BQ table {temp_tbl}")

        # Merge only new articles to main table
        destination_tbl = (f"{self.project_id}.curated_byte.{clean_sector[self.sector]}_articles"
                           if not self.test_run else
                           f"{self.project_id}.temporary.byte_{clean_sector[self.sector]}_articles"
        )
                
        merge_sql = f"""
                    MERGE `{destination_tbl}` a
                    USING `{temp_tbl}` b
                    ON a.id = b.id
                    WHEN NOT MATCHED
                    THEN
                    INSERT (id, company_id, sector, url, title, body, source_name, pillar, dateTimePub, run_datetime)
                    VALUES (b.id, b.company_id, b.sector, b.url, b.title, b.body, b.source_name, b.pillar, b.dateTimePub, b.run_datetime)
        """
        
        merge_succeeded = False

        try:
            merge_job = self.bq_client.query(merge_sql)
            merge_job.result()
            num_inserted = merge_job.num_dml_affected_rows or 0
            print(f"Merged {num_inserted} new articles into: {destination_tbl}")
            merge_succeeded = True
        except Exception as e:
            print(f"Error running merge: {e}")
            return
        finally:
            if merge_succeeded:
                try:
                    self.bq_client.delete_table(temp_tbl, not_found_ok=True) # cleanup temp table
                    print(f"Deleted temp table: {temp_tbl}")
                except Exception as e:
                    print(f"Failed to delete temp table: {e}")

        # Collect all kept and removed articles' features for QA
        if self.qa_data:

            # Upload the relevant articles to BigQuery for QA
            table_id = (f"{self.project_id}.curated_byte.{clean_sector[self.sector]}_relevant_articles_QA"
                           if not self.test_run else
                        f"{self.project_id}.temporary.byte_{clean_sector[self.sector]}_relevant_articles_QA"
            )

            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE"
            )

            job = self.bq_client.load_table_from_dataframe(relevant_articles,
                                                            table_id,
                                                            job_config=job_config)
            job.result()
            print(f"Uploaded {len(relevant_articles)} rows to BQ table {table_id}")
            
            # Upload filtered articles to BigQuery for QA
            if not short_articles.empty:
                short_articles = short_articles.copy()
                short_articles['filtered_reason'] = 'short_article'
                qa_filtered_articles.append(short_articles)

            if not stock_picks.empty:
                stock_picks = stock_picks.copy()
                stock_picks['filtered_reason'] = 'stock_pick'
                qa_filtered_articles.append(stock_picks)

            if not internal_duplicates.empty:
                internal_duplicates = internal_duplicates.copy()
                internal_duplicates['filtered_reason'] = 'internal_duplicate'
                qa_filtered_articles.append(internal_duplicates)

            if not hist_duplicates.empty:
                hist_duplicates = hist_duplicates.copy()
                hist_duplicates['filtered_reason'] = 'historical_duplicate'
                qa_filtered_articles.append(hist_duplicates)    
            
            if not removed_by_features.empty:
                removed_by_features = removed_by_features.copy()
                removed_by_features['filtered_reason'] = 'heuristics_cutoffs'
                qa_filtered_articles.append(removed_by_features)    

            if not non_relevant_articles.empty:
                non_relevant_articles = non_relevant_articles.copy()
                non_relevant_articles['filtered_reason'] = 'llm_not_relevant'
                qa_filtered_articles.append(non_relevant_articles)  
            
            qa_dataset = pd.DataFrame()            
            qa_dataset = pd.concat(qa_filtered_articles, ignore_index=True)

            table_id = (f"{self.project_id}.curated_byte.{clean_sector[self.sector]}_filtered_articles_QA"
                           if not self.test_run else
                        f"{self.project_id}.temporary.byte_{clean_sector[self.sector]}_filtered_articles_QA"
            )
    
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE"
            )

            job = self.bq_client.load_table_from_dataframe(qa_dataset, table_id, job_config=job_config)
            job.result()
            print(f"Uploaded {len(qa_dataset)} rows to BQ table {table_id}")

        return relevant_articles
    