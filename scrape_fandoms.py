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

lco.init("config.yml")

if not Path(f"{lco['data_dir']}").exists():
    Path(f"{lco['data_dir']}").mkdir(parents=True)

if Path(f"{lco['data_dir']}/canonical_fandom_tags.csv").exists():
    print("canonical fandom tags already scraped")
    exit()


# fandom url
fandom_url = "https://archiveofourown.org/tags/search?tag_search%5Bname%5D=&tag_search%5Bfandoms%5D=&tag_search%5Btype%5D=Fandom&tag_search%5Bcanonical%5D=T&tag_search%5Bsort_column%5D=created_at&tag_search%5Bsort_direction%5D=asc&commit=Search+Tags"

# first request, to get number of pages
response = scrape_url(fandom_url)
soup = BeautifulSoup(response.text, "html.parser")

# get number of pages
last_page = int(soup.find("ol", class_="pagination actions").find_all("li")[-2].text)

print(f"found {last_page} pages for canonical fandom tags")

pages_to_scrape = []

# scrape tags
for page in range(1, last_page + 1):
    pages_to_scrape.append(fandom_url + f"&page={page}")

def scrape_page(url):
    response = scrape_url(url)
    soup = BeautifulSoup(response.text, "html.parser")
    tags = soup.find_all("span", class_="canonical")
    results = []
    if len(tags) == 0:
        return None
    for tag in tags:
        full_text = tag.text
        # get <a> tag
        a_tag = tag.find("a")
        tag_text = a_tag.text
        count = full_text.split(tag_text)[1].replace("(", "").replace(")", "").strip()
        count = "".join([c for c in count if c.isdigit()])
        count = int(count)
        # get href
        href = a_tag["href"]
        digest = hashlib.sha256(href.encode()).hexdigest()
        tag_hash = humanhash.humanize(digest, words=2)
        results.append((tag_text, count, href, tag_hash))
    return results

all_results = []

if lco["only_scrape_first_n_pages"] > 0:
    pages_to_scrape = pages_to_scrape[:lco["only_scrape_first_n_pages"]]

executor = ThreadPoolExecutor(lco["num_threads"])

# for page in tqdm(pages_to_scrape):
#     results = scrape_page(page)
#     while results is None:
#         results = scrape_page(page)
#         time.sleep(lco["fail_interval"])
#     all_results += results

for results in tqdm(executor.map(scrape_page, pages_to_scrape), total=len(pages_to_scrape)):
    while results is None:
        results = scrape_page(page)
        time.sleep(lco["fail_interval"])
    all_results += results

df = pd.DataFrame(all_results, columns=["tag", "count", "href", "hash"])

df.to_csv(f"{lco['data_dir']}/canonical_fandom_tags.csv", index=False)