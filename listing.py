import requests
from bs4 import BeautifulSoup

def get_warc_gz_links():
    index_url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-13/index.html"
    response = requests.get(index_url)
    # Use 'xml' parser here instead of 'html.parser'
    soup = BeautifulSoup(response.text, 'xml')  
    warc_links = [a.text for a in soup.find_all('loc') if a.text.endswith('.warc.gz')]
    return warc_links

warc_files = get_warc_gz_links()
print(f"Found {len(warc_files)} WARC files")
print(warc_files[:5])  # print first 5 links to verify


