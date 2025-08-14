from datetime import datetime
from workflows_byte.summarisation import ByteSummarisation

def run_byte_workflow(sector):
    workflow = ByteSummarisation(sector=sector,
                            test_run=False, 
                            )
    
    articles = workflow.trigger_workflow()
    return articles

def main():
    sectors = ["Index Bank"] # only doing banking for now
    
    for sector in sectors:
        articles = run_byte_workflow(sector=sector)
        print(articles.head())

if __name__ == "__main__":
    main()