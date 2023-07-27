import lco
from bs4 import BeautifulSoup
from util import scrape_url
from rich import print
import pandas as pd
from pathlib import Path
from tqdm.auto import tqdm
import humanhash
import hashlib
import time
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from urllib.parse import quote_plus

lco.init("config.yml")

tag_path = Path(f"{lco['data_dir']}/tags")
if not tag_path.exists():
    tag_path.mkdir(parents=True)

fandom_df = pd.read_csv(f"{lco['process_dir']}/canonical_fandoms.csv")

fandom_df = fandom_df.sort_values("count", ascending=True)

tags_url = "https://archiveofourown.org/tags/search?tag_search[name]=&tag_search[fandoms]={fandom}&tag_search[type]=&tag_search[canonical]=T&tag_search[sort_column]=created_at&tag_search[sort_direction]=asc&commit=Search+Tags"

def get_tags(soup):
    tags = soup.find_all("span", class_="canonical")
    results = []
    if len(tags) == 0:
        return None
    for tag in tags:
        # get <a> tag
        a_tag = tag.find("a")
        tag_text = a_tag.text
        inner_html = str(tag)
        # split on a tag
        category, count = inner_html.split(str(a_tag))
        category = category.split(">")[-1][0].lower()
        count = "".join([c for c in count if c.isdigit()])
        if len(count) == 0:
            count = 0
            print(f"no count for {tag_text} on {url}")
        count = int(count)
        # get href
        href = a_tag["href"]
        results.append((tag_text, count, href, category))
    return results

def scrape_page(url):
    response = scrape_url(url)
    soup = BeautifulSoup(response.text, "html.parser")
    results = get_tags(soup)
    return results

num_threads = lco["num_threads"]

for _, row in tqdm(fandom_df.iterrows(), total=len(fandom_df)):
    if row["count"] < 100:
        continue
    fandom = quote_plus(row["tag"])
    fandom_path = Path(f"{tag_path}/{row['hash']}")
    if fandom_path.exists():
        print(f"{fandom} already scraped")
        continue
    response = scrape_url(tags_url.format(fandom=fandom))
    soup = BeautifulSoup(response.text, "html.parser")
    pagination = soup.find("ol", class_="pagination actions")
    if pagination is None:
        print(f"no pages for {fandom}")
        results = get_tags(soup)
        if results is None:
            print(f"no tags for {fandom}")
            # write empty file
            fandom_path.mkdir(parents=True)
            with open(f"{fandom_path}/tags.csv", "w") as f:
                f.write("")
            continue
        else:
            df = pd.DataFrame(results, columns=["tag", "count", "href", "category"])
            fandom_path.mkdir(parents=True)
            df.to_csv(f"{fandom_path}/tags.csv", index=False)
        continue
    num_pages = int(pagination.find_all("li")[-2].text)
    print(f"found {num_pages} pages for {fandom}")
    pages_to_scrape = []
    # scrape tags
    for page in range(1, num_pages + 1):
        pages_to_scrape.append(tags_url.format(fandom=fandom) + f"&page={page}")
    results = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        for result in tqdm(executor.map(scrape_page, pages_to_scrape), total=len(pages_to_scrape), desc=fandom):
            if result is not None:
                results += result
    df = pd.DataFrame(results, columns=["tag", "count", "href", "category"])
    fandom_path.mkdir(parents=True)
    df.to_csv(f"{fandom_path}/tags.csv", index=False)