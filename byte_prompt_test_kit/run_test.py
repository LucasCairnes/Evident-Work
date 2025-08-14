from byte_summarisation import ByteSummarisation
import pandas as pd

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
        articles.to_excel("lucas_prompt_testing/input_output/summarisation_test_output.xlsx")

if __name__ == "__main__":
    main()