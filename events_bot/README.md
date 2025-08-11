# MarketScreener Events Pipeline

This directory contains scripts for scraping, cleaning, and curating company event data from MarketScreener, and loading it to evident-data-dev

---

## Files

### `date_scraper.py`

**Purpose:**  
Asynchronously scrapes upcoming event data from MarketScreener calendar links stored at `evident-data-dev.raw_marketscreener.calendar_links`.

**Workflow:**
- Loads calendar URLs from `evident-data-dev.raw_marketscreener.calendar_links`.
- Fetches and parses event data for each company using async HTTP requests.
- Merges scraped events with company metadata.
- Cleans and formats the data.
- Uploads the raw events to `evident-data-dev.raw_marketscreener.events` in BigQuery.

---
    
### `post_processing.py`

**Purpose:**  
Cleans and filters the raw events data for slack bot use

**Workflow:**
- Loads raw events from `evident-data-dev.raw_marketscreener.events`.
- Standardizes event names and removes uninteresting events (e.g., dividends).
- Outputs curated events to `evident-data-dev.curated_marketscreener.events` in BigQuery.

---

## Usage

1. **Run `date_scraper.py`** to fetch and store the latest events.
2. **Run `post_processing.py`** to clean and curate the events for analysis.

---

## Requirements

- Python 3.7+
- Google Cloud BigQuery Python client
- `pandas`, `aiohttp`, `tqdm`, `requests`, `bs4`
- Evident's internal `dolly` and `load_dataframe_to_table` utilities
