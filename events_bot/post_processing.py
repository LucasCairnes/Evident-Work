import pandas as pd
from google.cloud import bigquery
from evident.ds import dolly
from evident.bigquery import load_dataframe_to_table

# Initialize BigQuery client and load raw events data
bq_client = bigquery.Client(project='evident-data-dev')
events_id = "evident-data-dev.raw_marketscreener.events"
processed_events = bq_client.list_rows(events_id).to_dataframe()

def clean_earnings_calls(event_name):
    """
    Standardize event names by removing or replacing common substrings.
    """
    event_name = event_name.replace('Release', 'Call')
    event_name = event_name.replace('(Projected)', '')
    event_name = event_name.replace('Pre-market', '')
    return event_name.strip()

def clean_events(df):
    """
    Filter out uninteresting events and return relevant columns.
    """
    events_to_filter_kws = {'NOT_INTERESTING': ['dividend', 'Détachement']}
    df['not_interesting'] = dolly.keywords_over_text(df, 'event', events_to_filter_kws)['tags']
    df = df[df.not_interesting.apply(lambda x: len(x) == 0)]
    return df[['date', 'event', 'name']].reset_index(drop=True)

# Clean and filter event names
processed_events['event'] = processed_events['event'].apply(clean_earnings_calls)
filtered_events = clean_events(processed_events)

# Upload the curated events to BigQuery
load_dataframe_to_table(
    filtered_events,
    'evident-data-dev.curated_marketscreener.events'
)
print("uploaded")
