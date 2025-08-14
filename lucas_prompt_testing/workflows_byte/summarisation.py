import os
import pandas as pd 
from typing import Literal, Optional
from google.cloud import bigquery
from datetime import datetime, timedelta
import asyncio
from dotenv import load_dotenv
load_dotenv()

from src.news_package.processing.fuzzy_deduplication import remove_internal_duplicates
from src.news_package.LLM_summarisation.summarise import GeminiArticleSummariser
from src.news_package.LLM_summarisation.summary_prompts import get_summary_prompt


class ByteSummarisation(object):
    def __init__(self,
                 sector: Literal["Index Bank", "Insurance1000", "Index Insurance", "Payments", "Index1000", "Other"],
                 start_date=None, # datetime for when to start article search from, default is X days ago, where X = number_of_days. - example input: datetime(2025, 7, 1, 0, 0)
                 end_date=None, # datetime cutoff for article search, default is current timestamp today - example input: datetime(2025, 7, 15, 23, 59)
                 number_of_days: Optional[int] = 7, # see start_date #TODO: change this desc to publication date bc of query in get_articles_df()
                 test_run=False,
                 ):
        
        # constants
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT") # set gcloud project inside the .env file
        if not self.project_id:
            raise EnvironmentError("GOOGLE_CLOUD_PROJECT is not set in the environment or .env file.")
        
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
        
        self.sector = sector
        self.test_run = test_run
        self.bq_client = bigquery.Client(project=self.project_id)

    def get_articles_df(self):
        """
        get articles from intermediate table in bigquery and create pandas df.
        """

        clean_sector = {"Index Bank": "banking",
                        "Index Insurance": "insurance",
                        "Payments": "payments"}

        source_tbl = (f"{self.project_id}.curated_byte.{clean_sector[self.sector]}_articles"
                           if not self.test_run else
                           f"{self.project_id}.temporary.byte_{clean_sector[self.sector]}_articles"
        )

        query = f"""
            SELECT
            * except(run_datetime)
            FROM `{source_tbl}`
            WHERE 1=1
            AND DATE(dateTimePub) >=  DATE('{self.start_date}')
            AND DATE(dateTimePub) <= DATE_ADD(DATE('{self.start_date}'), INTERVAL '{self.number_of_days}' DAY)
        """

        query_job = self.bq_client.query(query)
        df = query_job.to_dataframe()

        if df.empty:
            print("no articles in df")
        else:
            print(f"articles df created from {source_tbl}")
            print(df.head())
        
        return df
    
    
    async def summarise(self, articles):
        summariser = GeminiArticleSummariser(summarisation_prompt=get_summary_prompt(self.sector), project_id=self.project_id)
        coroutines = [summariser.generate_summary(text) for text in articles.body.values]
        summaries = await asyncio.gather(*coroutines)
        return summaries

    def trigger_workflow(self):
        articles = self.get_articles_df()
        articles["run_datetime"] = datetime.now()

        print(f"Number Of Articles: {articles.shape[0]}")

        # summarise
        summaries = asyncio.run(self.summarise(articles=articles))
        articles['summary'] = summaries

        # perform final deduplication based on the article summary text
        articles, summary_duplicates = remove_internal_duplicates(articles, 
                                                                   embedding_model='all-mpnet-base-v2',
                                                                   body_field='summary',
                                                                   ranking_field='company_id', # TODO: change this to source importance or num ai mentions etc or text length 
                                                                   title_similarity_threshold=0.8,
                                                                   body_similarity_threshold=0.8)
        print(f"After Removing Duplicate Summaries: {articles.shape[0]}")

        
        ### Uploading to BigQuery ###
    
        # Ensure proper dtypes before upload
        articles["company_id"] = pd.to_numeric(articles["company_id"], errors="coerce")  # Converts to float64 or int
        articles["dateTimePub"] = pd.to_datetime(articles["dateTimePub"], errors="coerce")
        articles["datePub"] = articles["dateTimePub"].dt.date
        articles["run_datetime"] = pd.to_datetime(articles["run_datetime"], errors="coerce")
        # reorder cols 
        articles = articles[["id", "company_id", "sector", "source_name", "pillar", "url", "title", "body", "summary", "datePub", "dateTimePub", "run_datetime"]]

        schema = [
                    bigquery.SchemaField("id", "STRING"),
                    bigquery.SchemaField("company_id", "INTEGER"),
                    bigquery.SchemaField("sector", "STRING"),
                    bigquery.SchemaField("source_name", "STRING"),
                    bigquery.SchemaField("pillar", "STRING"),
                    bigquery.SchemaField("url", "STRING"),
                    bigquery.SchemaField("title", "STRING"),
                    bigquery.SchemaField("body", "STRING"),
                    bigquery.SchemaField("summary", "STRING"),
                    bigquery.SchemaField("datePub", "DATE"),
                    bigquery.SchemaField("dateTimePub", "TIMESTAMP"),
                    bigquery.SchemaField("run_datetime", "TIMESTAMP"),
                ]

        clean_sector = {"Index Bank": "banking",
                        "Index Insurance": "insurance",
                        "Payments": "payments"}

        temp_tbl = (f"{self.project_id}.product_byte.{clean_sector[self.sector]}_article_summaries_temp"
                           if not self.test_run else
                           f"{self.project_id}.temporary.byte_{clean_sector[self.sector]}_article_summaries_temp"
        )
        
        job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition="WRITE_TRUNCATE"
            )

        job = self.bq_client.load_table_from_dataframe(articles, temp_tbl, job_config=job_config)
        job.result()
        print(f"Uploaded {len(articles)} rows to BQ table {temp_tbl}") 

        # Merge only new articles to main table
        destination_tbl = (f"{self.project_id}.product_byte.{clean_sector[self.sector]}_article_summaries"
                           if not self.test_run else
                           f"{self.project_id}.temporary.byte_{clean_sector[self.sector]}_article_summaries"
        )
                
        merge_sql = f"""
                    MERGE `{destination_tbl}` a
                    USING `{temp_tbl}` b
                    ON a.id = b.id
                    WHEN NOT MATCHED
                    THEN
                    INSERT (id, company_id, sector, source_name, pillar, url, title, body, summary, datePub, dateTimePub, run_datetime)
                    VALUES (b.id, b.company_id, b.sector, b.source_name, b.pillar, b.url, b.title, b.body, b.summary, b.datePub, b.dateTimePub, b.run_datetime)
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

        # output any filtered articles for QA
        if not summary_duplicates.empty:
            qa_filtered_articles = []

            summary_duplicates = summary_duplicates.copy()
            summary_duplicates['filtered_reason'] = 'summary_duplicate'
            qa_filtered_articles.append(summary_duplicates)     
            qa_dataset = pd.DataFrame()            
            qa_dataset = pd.concat(qa_filtered_articles, ignore_index=True)
            
            table_id = (f"{self.project_id}.curated_byte.{clean_sector[self.sector]}_article_summary_duplicates_QA"
                           if not self.test_run else
                           f"{self.project_id}.temporary.byte_{clean_sector[self.sector]}_article_summary_duplicates_QA"
        )

            job_config = bigquery.LoadJobConfig(
                # schema=schema,
                write_disposition="WRITE_TRUNCATE"
            )

            job = self.bq_client.load_table_from_dataframe(qa_dataset, table_id, job_config=job_config)
            job.result()
            print(f"Uploaded {len(qa_dataset)} rows to BQ table {table_id}")
        else:
            print("No summary duplicates found, no QA data uploaded.")
