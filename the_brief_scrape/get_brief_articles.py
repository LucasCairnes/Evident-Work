import asyncio
import httpx
from bs4 import BeautifulSoup
import pandas as pd

import gspread


base_url = "https://evidentinsights.com"
sub_section = "/bankingbrief/"

def get_article_links():
    page_html = httpx.get(base_url+sub_section).text
    soup = BeautifulSoup(page_html, "html.parser")

    article_links = [article.get('href') for article in
                     soup.find_all(class_ = "text-black transition-colors no-underline")
                     if article.get('href')]
    return article_links

async def fetch_article_text(client, url_section):
    html_response = await client.get(base_url + url_section)
    soup = BeautifulSoup(html_response.text, "html.parser")
    story_sections = soup.find_all(class_ = "nws-container")

    unwanted_id = ("whats-on-at-evident", "about-evident") 
    unwanted_h2 = ("THE BRIEF TEAM")

    filtered_sections = []
    for section in story_sections:
        if section.get('id') in unwanted_id:
            continue
        
        h2_tag = section.find('h2')
        if h2_tag and h2_tag.text.strip() in unwanted_h2:
            continue
        
        filtered_sections.append(section.get_text(strip=True, separator=' '))
    
    return " ".join(filtered_sections)

async def main(links):
    semaphore = asyncio.Semaphore(10)

    async def fetch_with_semaphore(client, link):
        async with semaphore:
            return await fetch_article_text(client, link)

    async with httpx.AsyncClient() as client:

        tasks = [fetch_with_semaphore(client, link) for link in links]
        results = await asyncio.gather(*tasks)
        
        combined_data = dict(zip(links, results))
        return combined_data

if __name__ == "__main__":
    article_links = get_article_links()
    
    fetched_articles_map = asyncio.run(main(article_links))
    
    df = pd.DataFrame(fetched_articles_map.items(), columns=['link', 'content'])

df.to_excel("brief_editions_with_articles.xlsx", index=False)