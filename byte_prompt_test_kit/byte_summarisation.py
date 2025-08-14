import pandas as pd 
from typing import Literal
import asyncio
from dotenv import load_dotenv
load_dotenv()

from gemini_summariser import GeminiArticleSummariser
from summary_prompts import get_summary_prompt

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

        articles = pd.read_excel("input_output/articles_input.xlsx")
        articles['date_published'] = articles['date_published'].dt.strftime('%d-%b-%Y')


        print(f"Number Of Articles: {articles.shape[0]}")

        # summarise
        summaries = asyncio.run(self.summarise(articles=articles))
        articles['summary'] = summaries

        # reorder cols 
        articles = articles[["url", "title", "body", "source_name", "date_published", "pillar", "summary"]]
        return articles
