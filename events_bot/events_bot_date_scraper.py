import requests
from random import randint
import pandas as pd
from bs4 import BeautifulSoup
import time
from evident.bigquery import load_dataframe_to_table

from google.cloud import bigquery
from datetime import datetime
from tqdm import tqdm
import asyncio
import aiohttp

# Initialize BigQuery client and load calendar URLs
bq_client = bigquery.Client(project='evident-data-dev')
calendar_links_id = "evident-data-dev.raw_marketscreener.calendar_links"
calendar_urls = bq_client.list_rows(calendar_links_id).to_dataframe()

# Set up rate limiting for async requests
RATE_LIMIT = 10
semaphore = asyncio.Semaphore(RATE_LIMIT)

# HTTP headers for requests to MarketScreener
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'max-age=0'
}

def clean_event(event):
    """Remove extra whitespace from event text."""
    event = event.strip()
    return "".join(event.split('  '))

async def fetch_event_data(session, url, progress_bar):
    """
    Asynchronously fetch and parse event data from a MarketScreener calendar URL.
    Returns a DataFrame with columns: url, date, event.
    """
    async with semaphore:
        try:
            await asyncio.sleep(randint(1, 3))  # Random sleep to avoid rate-limiting
            async with session.get(
                url=url,
                headers=HEADERS,
                timeout=30
            ) as response:
                if response.status == 200:
                    html_content = await response.text()
                    soup = BeautifulSoup(html_content, 'html.parser')
                    events_card = soup.find("div", {"id": "next-events-card"})
                    if events_card:
                        upcoming_events = [event.text for event in events_card.find_all('tr')]
                        clean_events = [clean_event(event) for event in upcoming_events]
                        dates = [x.split('\n\n')[0].split('\n')[0] for x in clean_events]
                        event_names = [x.split('\n\n')[1] for x in clean_events]
                    else:
                        dates = []
                        event_names = []
                else:
                    print(f"Error: Status code {response.status} for URL {url}")
                    dates = []
                    event_names = []
                progress_bar.update(1)
                return pd.DataFrame({'url': url, 'date': dates, 'event': event_names})
        except asyncio.TimeoutError:
            print(f"Timeout error for URL {url}")
            progress_bar.update(1)
            return pd.DataFrame({'url': url, 'date': [], 'event': []})
        except Exception as e:
            print(f"Error fetching data for URL {url}: {e}")
            progress_bar.update(1)
            return pd.DataFrame({'url': url, 'date': [], 'event': []})

async def fetch_marketscreener_urls(urls):
    """
    Orchestrates asynchronous fetching of event data for all provided URLs.
    """
    connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        with tqdm(total=len(urls)) as progress_bar:
            tasks = [fetch_event_data(session, url, progress_bar) for url in urls]
            results = await asyncio.gather(*tasks)
        return results

async def main():
    """
    Main async entry point: fetch all events for all calendar URLs.
    """
    urls = calendar_urls.market_screener_link.values
    start_time = time.time()
    events_data = await fetch_marketscreener_urls(urls)
    print(f"Completed {len(urls)} requests in {time.time() - start_time} seconds.")
    return events_data

if __name__ == "__main__":
    # Run async scraping and aggregate results
    events_data = pd.concat(asyncio.run(main()))
    # Merge scraped events with company metadata
    company_events_data = events_data.merge(
        calendar_urls[['market_screener_link', 'company_id', 'name']],
        left_on='url',
        right_on='market_screener_link',
        how='left'
    )
    # Parse event dates and add run date
    company_events_data['date'] = pd.to_datetime(
        company_events_data['date'], format="%d/%m/%Y", utc=False
    ).dt.strftime('%Y/%m/%d')
    # Select only required columns
    output_df = company_events_data[['name', 'date', 'company_id', 'event']]
    # Remove duplicates and reset index
    output_df.drop_duplicates(inplace=True)
    output_df.reset_index(drop=True, inplace=True)
    # Upload results to BigQuery
    load_dataframe_to_table(
        output_df,
        'evident-data-dev.raw_marketscreener.events'
    )
    print("uploaded")
