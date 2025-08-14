
from dotenv import load_dotenv
load_dotenv()
import os
import json
from typing import Literal
from google.cloud import bigquery
from datetime import datetime, timedelta
from src.news_package.data.newsapi import login_eventregistry, fetch_company_concepts_list, article_search_and_return_list_of_dicts, refresh_source_uri_tbl_in_bigquery


class ByteIngestion(object):
    def __init__(self,
                 sector: Literal["Index Bank", "Insurance1000", "Index Insurance", "Payments", "Index1000", "Other"],
                 skip_refresh_source_uri=False,
                 skip_ingestion_step = False,
                 output_articles_as_json = False,
                 start_date=None,
                 end_date=None,
                 number_of_days=3,
                 test_run=False,
                 ):
        
        # constants
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT") # set gcloud project inside the .env file
        if not self.project_id:
            raise EnvironmentError("GOOGLE_CLOUD_PROJECT is not set in the environment or .env file.")
        self.sector = sector
        self.skip_refresh_source_uri=skip_refresh_source_uri
        self.skip_ingestion_step = skip_ingestion_step
        self.output_articles_as_json = output_articles_as_json
        if not number_of_days:
            self.number_of_days = 3
        else:
            self.number_of_days = number_of_days
        if not end_date:
            self.end_date = datetime.now()
        else:
            self.end_date = end_date
        if not start_date:
            self.start_date = (self.end_date - timedelta(days=self.number_of_days)).strftime('%Y-%m-%d')
        else:
            self.start_date = start_date
        self.test_run = test_run
        self.bq_client = bigquery.Client(project=self.project_id)

    def retrieve_articles_from_news_api(self):

        companies_to_search = fetch_company_concepts_list(sector=self.sector, project_id=self.project_id)

        er = login_eventregistry()
        
        daily_articles_all_companies = []

        for index, row in enumerate(companies_to_search):
            try:
                daily_articles = article_search_and_return_list_of_dicts(
                                                                        search_concepts=row['url_list'],
                                                                        eventregistry_client=er,
                                                                        date_start=self.start_date, 
                                                                        date_end=self.end_date.strftime('%Y-%m-%d'),
                                                                        company_id=row['id'],
                                                                        sector=self.sector
                                                                        )
                daily_articles_all_companies.extend(daily_articles)

            except Exception as e:
                print(f"Exception: {e}")
                print(f"Error processing data: {row}")
                continue

        if not daily_articles_all_companies:
            print("No articles picked up from newsapi.")
        else:
            print("Picked up {0} articles from newsapi for the sector: {1}...".format(len(daily_articles_all_companies), self.sector))
            # print(json.dumps(daily_articles_all_companies[0], indent=2))

            if self.output_articles_as_json:
                print(f"Keys in one article: {list(daily_articles_all_companies[0].keys())}")

                for i, article in enumerate(daily_articles_all_companies[:3]):
                    print(f"Article {i+1}:")
                    print(json.dumps(article, indent=2))
                    print("-" * 40)
                
                output_dir = "newsapi_article_exports"
                os.makedirs(output_dir, exist_ok=True)

                json_file_path = os.path.join(output_dir, f"{self.sector}_articles_no_added_cols.json")

                # Write to JSON
                with open(json_file_path, "w", encoding="utf-8") as f_json:
                    json.dump(daily_articles_all_companies, f_json, indent=2, ensure_ascii=False)

                print(f"Wrote JSON output to: {json_file_path}")

        return daily_articles_all_companies


    def upload_articles_json_to_bigquery(self, articles):
            
        if not articles:
            print("No articles picked up from newsapi")
            return
        else:
            clean_sector = {"Index Bank": "banking",
                            "Index Insurance": "insurance",
                            "Payments": "payments"}

            # Upload all new articles from this run to a temp staging table
            temp_tbl = (f"{self.project_id}.raw_newsapi.{clean_sector[self.sector]}_articles_temp" 
                        if not self.test_run else 
                        f"{self.project_id}.temporary.byte_{clean_sector[self.sector]}_newsapi_articles_temp")
                        # all local and cloud test runs uploaded to evident-data-dev, all production runs uploaded to evident-data-prod

            records = [{"article_json": json.dumps(article, ensure_ascii=False)} for article in articles] # upload as JSON strings
            job_config = bigquery.LoadJobConfig(schema=[bigquery.SchemaField("article_json", "STRING")],
                                                write_disposition="WRITE_TRUNCATE")

            job = self.bq_client.load_table_from_json(records, temp_tbl, job_config=job_config)
            job.result()
            print(f"Uploaded {len(records)} articles to temp table: {temp_tbl}")

            # Merge only new articles to main table
            destination_tbl = (f"{self.project_id}.raw_newsapi.{clean_sector[self.sector]}_articles"
                        if not self.test_run else 
                        f"{self.project_id}.temporary.byte_{clean_sector[self.sector]}_newsapi_articles"
            )

            merge_sql = f"""
                MERGE `{destination_tbl}` a
                USING `{temp_tbl}` b
                ON JSON_EXTRACT_SCALAR(a.article_json, '$.temp_id') = JSON_EXTRACT_SCALAR(b.article_json, '$.temp_id')
                WHEN NOT MATCHED
                THEN
                INSERT (article_json)
                VALUES (b.article_json)
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
            


    def trigger_workflow(self):

        if self.skip_refresh_source_uri==True:
            print("Skipping refresh source uri step...")
        else:
            # update source uris for approved media outlets
            refresh_source_uri_tbl_in_bigquery(eventregistry_client=login_eventregistry(),project_id=self.project_id)

        if self.skip_ingestion_step==True:
            print("Skipping ingestion step...go to next workflow")
        else:
            self.upload_articles_json_to_bigquery(articles=self.retrieve_articles_from_news_api())

