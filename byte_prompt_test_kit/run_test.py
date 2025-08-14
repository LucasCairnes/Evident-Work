from byte_summarisation import ByteSummarisation
import pandas as pd
from datetime import datetime

def run_byte_workflow(sector):
    workflow = ByteSummarisation(sector=sector,
                            test_run=False, 
                            )
    
    articles = workflow.trigger_workflow()
    return articles

def main():
    sectors = ["Index Bank"] 
    
    for sector in sectors:
        articles = run_byte_workflow(sector=sector)
        
        # outputting to local file rather than to BQ
        timestamp = datetime.now().strftime("%b%d_%H%M").lower()
        articles.to_excel(f"input_output/articles_output_{timestamp}.xlsx", index=False)

if __name__ == "__main__":
    main()