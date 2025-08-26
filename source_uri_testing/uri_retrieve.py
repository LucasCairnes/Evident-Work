import os
from dotenv import load_dotenv
load_dotenv()
from eventregistry import *
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone
import uuid
import hashlib
import tldextract


RETURN_INFO = ReturnInfo(articleInfo = ArticleInfoFlags(basicInfo = True,
                                                    title = True,
                                                    body = True,
                                                    url = True,
                                                    eventUri = True,
                                                    authors = True,
                                                    concepts = True,
                                                    categories = True,
                                                    links = True,
                                                    videos = True,
                                                    image = True,
                                                    socialScore = True,
                                                    sentiment = True,
                                                    location = True,
                                                    dates = True,
                                                    extractedDates = True,
                                                    originalArticle = False,
                                                    storyUri = True),
                        sourceInfo = SourceInfoFlags(title = True,
                                                    description = True,
                                                    location = True,
                                                    ranking = True,
                                                    articleCount = True),
                                            
                        )

# TODO: make note to add more AI concepts to this list
ARTIFICIAL_INTELLIGENCE_CONCEPTS = ["https://en.wikipedia.org/wiki/Artificial_intelligence",
                                    "https://en.wikipedia.org/wiki/Machine_learning",
                                    "https://en.wikipedia.org/wiki/Natural_language_processing",
                                    "https://en.wikipedia.org/wiki/Deep_learning",
                                    "https://en.wikipedia.org/wiki/Chatbot",
                                    "http://en.wikipedia.org/wiki/Virtual_assistant",
                                    "http://en.wikipedia.org/wiki/Generative_model",
                                    "https://en.wikipedia.org/wiki/Generative_artificial_intelligence",
                                    "https://en.wikipedia.org/wiki/Reinforcement_learning",
                                    "https://en.wikipedia.org/wiki/Large_language_model",
                                    "http://en.wikipedia.org/wiki/ChatGPT",
                                    "http://en.wikipedia.org/wiki/GPT-4",
                                    "http://en.wikipedia.org/wiki/OpenAI",
                                    "http://en.wikipedia.org/wiki/Computer_vision"
                                    ]


def fetch_company_concepts_list(sector: Literal["Index Bank", "Insurance1000", "Index Insurance", "Payments", "Index1000", "Other"],
                                project_id):
    
    client=bigquery.Client(project=project_id)

    normalised_sector = sector.lower()
    
    concept_query = f"""
        SELECT
                t1.id,
                t1.name,
                t1.additional_names,
                t1.company_type,
                t1.internal_classification,
                t2.url_list
            FROM
                `evident-data.taxonomies.bank_metadata` AS t1
            LEFT JOIN
                (
                    SELECT id, ARRAY(SELECT url FROM UNNEST(concept_url) WHERE url IS NOT NULL) AS url_list
                    FROM `evident-data.staging.company_ids`
                    WHERE ARRAY_LENGTH(concept_url) > 0
                    AND EXISTS (SELECT 1 FROM UNNEST(concept_url) WHERE url IS NOT NULL)
                ) AS t2
            ON
                t1.id = t2.id
            WHERE lower(t1.internal_classification) = "{normalised_sector}"
    """

    results = client.query(concept_query).result()
    company_concepts_list = [dict(row) for row in results]

    total_urls = sum(len(row['url_list']) for row in company_concepts_list if row['url_list'])
    print(f"Fetched {total_urls} URLs for {len(company_concepts_list)} companies.")


    return company_concepts_list


def login_eventregistry():
    api_key = os.getenv("NEWSAPI_API_KEY")
    if not api_key:
        raise EnvironmentError("Missing or incorrect NEWSAPI_API_KEY in environment variables.")
    return EventRegistry(apiKey=api_key)


def article_search_and_return_list_of_dicts(search_concepts,
                                            eventregistry_client,
                                            date_start,
                                            date_end,
                                            company_id,
                                            sector,
                                            topic_concepts=ARTIFICIAL_INTELLIGENCE_CONCEPTS,
                                            return_info=RETURN_INFO,
                                            ):

    qStr ={
    "$query": {
        "$and": [
            { "dateStart": date_start, # find articles that were written on or after dateStart
                "dateEnd":date_end, # find articles that occurred before or on dateEnd
                "lang": "eng",
                "dataType": ['news', 'pr'],
            },
            {
                "conceptUri": {
                    "$or": topic_concepts
                },
            },
            {
                "conceptUri": {
                    "$or": search_concepts
                },
            }
        ]
    },
    }

    q= QueryArticlesIter.initWithComplexQuery(qStr)

    run_date = datetime.now(timezone.utc).isoformat()
    
    all_articles = []

    for article in q.execQuery(eventregistry_client,
                               returnInfo = return_info):

        article["article_id"] = str(uuid.uuid4())
        article["company_id"] = company_id
        article["sector"] = sector
        article["run_datetime"] = run_date 
        article["temp_id"] = hashlib.sha1(f"{article.get('dateTimePub', '')}_{article.get('url', '')}".encode("utf-8")).hexdigest()[:10]
        all_articles.append(article)

    return all_articles


def refresh_source_uri_tbl_in_bigquery(eventregistry_client, project_id):
    """
    Refresh the EventRegistry source URIs for each domain in the BQ table that stores all of the approved media sources we want to use in the byte.
    Adds a 'flag_missing_uri' column: 1 if URI not found, 0 if found.
    """
    bq_client = bigquery.Client(project=project_id)
    query = f"""
        SELECT 
          domain_name_root,	
          domain_name,	
          is_top_tier,	
          is_blacklisted,	
          ranking,	
          media_logo,	
          paywall, 
          partial_paywall,	
          is_company_site,	
          is_index,	
          company_id,	
          company_name,	
          sector
        FROM  `{project_id}.curated_byte.byte_news_outlets`
        WHERE domain_name IS NOT NULL
    """
    df = bq_client.query(query).to_dataframe()

    # Get source URIs from EventRegistry   
    uri_map = {}
    for outlet_domain in df["domain_name"].dropna().unique():
            cleaned_domain = str(tldextract.extract(outlet_domain).domain)
            if not cleaned_domain:
                print(f"Error extracting domain name from {outlet_domain}")
                cleaned_domain = outlet_domain
            try:
                uri = eventregistry_client.getSourceUri(cleaned_domain)
                if uri:
                    uri_map[cleaned_domain] = uri
                else:
                    print(f"Unmatched domain: {cleaned_domain}")
            except Exception as e:
                print(f"Error retrieving URI for {cleaned_domain}: {e}")

    df["eventregistry_sourceuri"] = df["domain_name"].map(uri_map)
    # Create flag: 1 if URI is missing or None
    df["flag_missing_uri"] = df["eventregistry_sourceuri"].isnull().astype(int)
    df["eventregistry_sourceuri"] = df["eventregistry_sourceuri"].fillna("")

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    bq_client.load_table_from_dataframe(df,
                                        f'{project_id}.temporary.lucas_uris_test', 
                                        job_config=job_config).result()

    print(f"Successfully refreshed `{project_id}.temporary.lucas_uris_test`")

event_reg_client = login_eventregistry()
project = 'evident-data-dev'

uri_test_run = refresh_source_uri_tbl_in_bigquery(event_reg_client, project)