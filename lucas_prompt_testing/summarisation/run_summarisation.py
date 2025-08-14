from datetime import datetime
from src.news_package.workflows_byte.summarisation import ByteSummarisation

def run_byte_workflow(sector):
    workflow = ByteSummarisation(sector=sector,
                            start_date=None, # datetime for when to start article search from, default is X days ago, where X = number_of_days. - example input: datetime(2025, 7, 1, 0, 0)
                            end_date=None, # datetime cutoff for article search, default is current timestamp today - example input: datetime(2025, 7, 15, 23, 59)
                            number_of_days=3, # see start_date #TODO: change this desc to publication date bc of query in get_articles_df()
                            test_run=False, # set to True when running a test, will upload data to staging_test dataset instead of staging
                            )
    
    articles = workflow.trigger_workflow()
    return articles

def main():
    sectors = ["Index Bank", 
               "Index Insurance"
            ]
    for sector in sectors:
        run_byte_workflow(sector=sector)

if __name__ == "__main__":
    main()

