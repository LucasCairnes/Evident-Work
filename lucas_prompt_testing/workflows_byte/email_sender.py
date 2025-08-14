import os
import pandas as pd
from typing import Literal, Optional
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from google.cloud import bigquery

from src.news_package.templates.html import HTML_START, HTML_END, ARTICLE_TEMPLATE

load_dotenv()

class ByteEmailSender:
    def __init__(self,
                 sector: Literal["Index Bank", "Insurance1000", "Index Insurance", "Payments", "Index1000", "Other"],
                 test_run: bool = False,
                 start_date=None,
                 end_date=None,
                 number_of_days=3,):
        
        self.sector = sector
        self.test_run = test_run
        self.sg = SendGridAPIClient(os.environ.get("SENDGRID_API_KEY"))
        self.list_id = "09d4f5cf-0ce2-4271-b066-f3bbc80f1021"
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT") # set gcloud project inside the .env file TODO: change to evident-data-dev eventually 
        self.bq_client = bigquery.Client(project=self.project_id)

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

    def generate_html(self, articles) -> str:
        article_html_list = []
        for _, article in articles.iterrows():
            article_html_list.append(
                ARTICLE_TEMPLATE.format(
                    article.url,
                    article.title,
                    article.source_name,
                    article.datePub,
                    article.summary.replace('\n', '<br /><br />')
                )
            )
        return HTML_START + "\n".join(article_html_list) + HTML_END

    def get_email_recipients(self) -> list:
        response = self.sg.client.marketing.contacts.get(query_params={'list_ids': self.list_id})
        contacts = response.to_dict.get('result', [])
        return [contact['email'] for contact in contacts]

    def send_email(self, html: str):
        message = Mail(
            from_email='data@evidentinsights.com',
            subject='The Byte',
            html_content=html
        )

        # Always send to core team
        message.add_to("sofia@evidentinsights.com")
        message.add_to("diogo@evidentinsights.com")

        # Add full list only if not a test run
        if not self.test_run:
            for email in self.get_email_recipients():
                message.add_to(email)

        response = self.sg.send(message)
        print(f"Email sent. Status code: {response.status_code}")

    def get_articles_df(self):
        """
        get articles from intermediate table in bigquery and create pandas df.
        """
        clean_sector = {"Index Bank": "banking",
                        "Index Insurance": "insurance",
                        "Payments": "payments"}

        source_tbl = (f"{self.project_id}.product_byte.{clean_sector[self.sector]}_article_summaries"
                           if not self.test_run else
                           f"{self.project_id}.temporary.byte_{clean_sector[self.sector]}_article_summaries"
        )

        query = f"""
            SELECT
            * except(run_datetime),
            id,
            company_id,
            sector,
            source_name,
            pillar,
            url,
            title,
            body,
            summary,
            datePub,
            dateTimePub,
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

    def trigger_workflow(self):
        articles = self.get_articles_df()
        print(f"Number Of Articles: {articles.shape[0]}")

        articles['run_datetime'] = datetime.now()

        email_html = self.generate_html(articles)
        self.send_email(email_html)
