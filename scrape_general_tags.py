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

file_path = Path(f"{lco['data_dir']}/tags/_general_tags.csv")

if file_path.exists():
    print("general tags already scraped")
    exit()

tags_url = "https://archiveofourown.org/tags/search?tag_search%5Bname%5D=&tag_search%5Bfandoms%5D=No+Fandom&tag_search%5Btype%5D=&tag_search%5Bcanonical%5D=T&tag_search%5Bsort_column%5D=created_at&tag_search%5Bsort_direction%5D=asc&commit=Search+Tags"

# first request, to get number of pages
response = scrape_url(tags_url)
soup = BeautifulSoup(response.text, "html.parser")

# get number of pages
last_page = int(soup.find("ol", class_="pagination actions").find_all("li")[-2].text)

print(f"found {last_page} pages for canonical nofandom tags")

pages_to_scrape = []

# scrape tags
for page in range(1, last_page + 1):
    pages_to_scrape.append(tags_url + f"&page={page}")

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
        # get innerhtml
        inner_html = str(tag)
        # split on a tag
        count = inner_html.split(str(a_tag))[1]
        count = "".join([c for c in count if c.isdigit()])
        if len(count) == 0:
            count = 0
            print(f"no count for {tag_text} on {url}")
        count = int(count)
        # get href
        href = a_tag["href"]
        results.append((tag_text, count, href))
    return results

results = []

num_threads = lco["num_threads"]

only_scrape_first_n_pages = lco["only_scrape_first_n_pages"]

if only_scrape_first_n_pages > 0:
    pages_to_scrape = pages_to_scrape[:only_scrape_first_n_pages]
    for result in tqdm(map(scrape_page, pages_to_scrape), total=len(pages_to_scrape)):
        if result is not None:
            results.extend(result)
else:
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        for result in tqdm(executor.map(scrape_page, pages_to_scrape), total=len(pages_to_scrape)):
            if result is not None:
                results.extend(result)
    
print(f"scraped {len(results)} tags")
print("saving to csv")
df = pd.DataFrame(results, columns=["tag", "count", "href"])
df.to_csv(file_path, index=False)