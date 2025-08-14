import os
import pandas as pd 
from typing import Literal, Optional
from google.cloud import bigquery
from datetime import datetime, timedelta
import asyncio
from dotenv import load_dotenv
load_dotenv()

from LLM_summarisation.summarise import GeminiArticleSummariser
from LLM_summarisation.summary_prompts import get_summary_prompt

# Notes: need to input a df called articles -> check what columns are needed
# Need to change where the files get outputted to

class ByteSummarisation(object):
    def __init__(self,
                 sector: Literal["Index Bank", "Insurance1000", "Index Insurance", "Payments", "Index1000", "Other"],                 test_run=False,
                 ):
        
        self.sector = sector
        self.test_run = test_run  
    
    async def summarise(self, articles):
        summariser = GeminiArticleSummariser(summarisation_prompt=get_summary_prompt(self.sector))
        coroutines = [summariser.generate_summary(text) for text in articles.body.values]
        summaries = await asyncio.gather(*coroutines)
        return summaries

    def trigger_workflow(self):

        articles = pd.read_excel("lucas_prompt_testing/input_output/summarisation_test_input.xlsx")
        articles["run_datetime"] = datetime.now()

        print(f"Number Of Articles: {articles.shape[0]}")

        # summarise
        summaries = asyncio.run(self.summarise(articles=articles))
        articles['summary'] = summaries
    
        # Ensure proper dtypes before upload
        articles["company_id"] = pd.to_numeric(articles["company_id"], errors="coerce")  # Converts to float64 or int
        articles["dateTimePub"] = pd.to_datetime(articles["dateTimePub"], errors="coerce")
        articles["datePub"] = articles["dateTimePub"].dt.date
        articles["run_datetime"] = pd.to_datetime(articles["run_datetime"], errors="coerce")
        # reorder cols 
        articles = articles[["id", "company_id", "sector", "source_name", "pillar", "url", "title", "body", "summary", "datePub", "dateTimePub", "run_datetime"]]
        return articles
