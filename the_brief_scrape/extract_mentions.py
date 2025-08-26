import pandas as pd
from google.cloud import bigquery

from get_brief_articles import run_scrape

articles_df = run_scrape()

project = 'evident-data-dev'

bq_client = bigquery.Client(project=project)

sql ='''
SELECT
  t.company,
  li.linkedin_organisation AS possible_names
FROM
  `evident-data-dev.curated_taxonomies.company_ids` AS t
CROSS JOIN
  UNNEST(t.linkedin_id) AS li
WHERE sector = 'bank'
'''

bq_df = bq_client.query(sql.format('bank')).to_dataframe()
bank_names_dict = bq_df.groupby('company')['possible_names'].apply(list).to_dict()

def company_mentions(text):
    mentions = []
    for company, possible_names in bank_names_dict.items():
        for name in possible_names:
            if name and name in text:
                mentions.append(company)
                break
    return mentions if mentions else []

articles_df["banks_mentioned"] = articles_df["content"].apply(company_mentions)

articles_df.to_excel("articles_with_mentions.xlsx")