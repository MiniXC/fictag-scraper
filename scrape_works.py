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
from datetime import datetime, timedelta

lco.init("config.yml")

work_path = Path(f"{lco['data_dir']}/works")

tag_path = Path(f"{lco['process_dir']}/tags")

if not work_path.exists():
    work_path.mkdir(parents=True)

fandom_df = pd.read_csv(f"{lco['process_dir']}/canonical_fandoms.csv")
fandom_df = fandom_df[fandom_df["count"] >= 100]
fandom_df = fandom_df[fandom_df["tag"] == "Sherlock (TV)"]

work_url = "https://archiveofourown.org/works/search?work_search%5Bquery%5D=&work_search%5Btitle%5D=&work_search%5Bcreators%5D=&work_search%5Brevised_at%5D={months_ago}+months+ago&work_search%5Bcomplete%5D=&work_search%5Bcrossover%5D=&work_search%5Bsingle_chapter%5D=0&work_search%5Bword_count%5D=&work_search%5Blanguage_id%5D=&work_search%5Bfandom_names%5D={fandom}&work_search%5Brating_ids%5D=&work_search%5Bcharacter_names%5D=&work_search%5Brelationship_names%5D=&work_search%5Bfreeform_names%5D=&work_search%5Bhits%5D=&work_search%5Bkudos_count%5D=&work_search%5Bcomments_count%5D=&work_search%5Bbookmarks_count%5D=&work_search%5Dsort_column%5D=revised_at&work_search%5Dsort_direction%5D=asc&commit=Search"

"""
Content rating

G
    General Audiences
T
    Teen And Up Audiences
M
    Mature
E
    Explicit: only suitable for adults
blank square
    The work was not given any rating

Relationships, pairings, orientations

F/F
    F/F: female/female relationships
F/M
    F/M: female/male relationships
Gen
    Gen: no romantic or sexual relationships, or relationships which are not the main focus of the work
M/M
    M/M: male/male relationships
Multi
    Multi: more than one kind of relationship, or a relationship with multiple partners
Other
    Other relationships
blank square
    The work was not put in any categories

Content warnings

questioned exclamation mark
    The author chose not to warn for content, or Archive Warnings could apply, but the author has chosen not to specify them. 
exclamation mark
    At least one of these warnings applies: graphic depictions of violence, major character death, rape/non-con, underage sex. The specific warnings are shown in the Archive Warnings tags. 
blank square
    The work was not marked with any Archive Warnings. Please note that an author may have included other information about their work in the Additional Tags (Genre, Warnings, Other Information) section.
globe
    This is an external work; please consult the work itself for warnings.

Is the work finished or the prompt fulfilled?

stop sign
    This is a work in progress or is incomplete/unfulfilled.
ticky
    This work is completed!/This prompt is filled!
blank square
    This work's status is unknown.
"""

# we only use one byte for encoding
enc_rating = {
    "General Audiences": "G",
    "Teen And Up Audiences": "T",
    "Mature": "M",
    "Explicit": "E",
    "Not Rated": "N",
}

enc_category = {
    "F/F": "F",
    "F/M": "H",
    "Gen": "G",
    "M/M": "M",
    "Multi": "U",
    "No category": "0",
    "Other": "O",
}

enc_warnings = {
    "Choose Not To Use Archive Warnings": "C",
    "Graphic Depictions Of Violence": "V",
    "Major Character Death": "D",
    "No Archive Warnings Apply": "N",
    "Rape/Non-Con": "R",
    "Underage": "U",
}

enc_status = {
    "Work in Progress": "W",
    "Complete Work": "C",
    "Unknown": "U",
}


def get_works(soup, fandom_tags, general_tags):
    works = soup.find_all("li", class_="work")
    results = []
    if len(works) == 0:
        return None
    for work in works:
        rating = work.find("span", class_="rating").text
        if "," in rating:
            rating = rating.split(",")
            rating = [r.strip() for r in rating]
        else:
            rating = [rating]
        rating = [enc_rating[r] for r in rating]
        rating = "".join(rating)
        category = work.find("span", class_="category").text
        if "," in category:
            category = category.split(",")
            category = [c.strip() for c in category]
        else:
            category = [category]
        category = [enc_category[c] for c in category]
        category = "".join(category)
        warnings = work.find("span", class_="warnings").text
        if "," in warnings:
            warnings = warnings.split(",")
            warnings = [w.strip() for w in warnings]
        else:
            warnings = [warnings]
        warnings = [enc_warnings[w] for w in warnings]
        warnings = "".join(warnings)
        status = work.find("span", class_="iswip").text
        status = status.strip()
        status = enc_status[status]
        heading = work.find("h4", class_="heading")
        # first <a> tag is title, second is author
        title = heading.find_all("a")[0].text
        # work id is in the href
        work_id = int(heading.find_all("a")[0]["href"].split("/")[-1])
        try:
            author = heading.find_all("a")[1].text
        except IndexError:
            author = "Anonymous"
        # hash author
        author_hash = hashlib.sha256(author.encode("utf-8")).hexdigest()
        # other fandoms
        fandoms = work.find("h5", class_="fandoms heading").find_all("a")
        if len(fandoms) > 1:
            fandoms = fandoms[1:]
        else:
            fandoms = []
        fandoms = [f.text for f in fandoms]
        # get tags
        tags = work.find("ul", class_="tags").find_all("li")
        tags = [t.text for t in tags if "warnings" not in t["class"]]
        fandom_tag_ids = []
        general_tag_ids = []
        for tag in tags:
            if tag in fandom_tags:
                fandom_tag_ids.append(fandom_tags[tag])
            elif tag in general_tags:
                general_tag_ids.append(general_tags[tag])
            else:
                pass
                #print(f"unknown tag {tag}")
        fandom_tag_ids = "+".join([str(t) for t in fandom_tag_ids])
        general_tag_ids = "+".join([str(t) for t in general_tag_ids])
        # language
        language = work.find("dd", class_="language")["lang"]
        # words
        words = work.find("dd", class_="words").text.strip().replace(",", "")
        # chapters
        chapters = work.find("dd", class_="chapters").text.strip()
        # comments
        try:
            comments = work.find("dd", class_="comments").text.strip().replace(",", "")
        except AttributeError:
            comments = 0
        # kudos
        try:
            kudos = work.find("dd", class_="kudos").text.strip().replace(",", "")
        except AttributeError:
            kudos = 0
        # bookmarks
        try:
            bookmarks = work.find("dd", class_="bookmarks").text.strip().replace(",", "")
        except AttributeError:
            bookmarks = 0
        # hits
        try:
            hits = work.find("dd", class_="hits").text.strip().replace(",", "")
        except AttributeError:
            hits = 0
        # date
        date = work.find("p", class_="datetime").text.strip()
        date = datetime.strptime(date, "%d %b %Y")
        date = date.strftime("%Y-%m-%d")
        results.append(
            (
                work_id,
                title,
                author_hash,
                fandoms,
                fandom_tag_ids,
                general_tag_ids,
                language,
                words,
                chapters,
                comments,
                kudos,
                bookmarks,
                hits,
                date,
                rating,
                category,
                warnings,
                status,
            )
        )
    if len(results) == 0:
        return None
    return results

def scrape_page(url):
    response = scrape_url(url)
    soup = BeautifulSoup(response.text, "html.parser")
    results = get_works(soup, fandom_tags, general_tags)
    return results

start_date = lco["start_month"]
end_date = lco["end_month"]

start_date = datetime.strptime(start_date, "%Y-%m")
end_date = datetime.strptime(end_date, "%Y-%m")

# create a dictionary of months to scrape in the form
# {months_ago: "YYYY-MM"}
months_to_scrape = {}
months_ago_start = (datetime.now() - start_date).days // 30
months_ago_end = (datetime.now() - end_date).days // 30
for months_ago in range(months_ago_end, months_ago_start + 1):
    date = datetime.now() - timedelta(days=months_ago * 30)
    months_to_scrape[months_ago] = date.strftime("%Y-%m")

for _, row in tqdm(fandom_df.iterrows(), total=len(fandom_df)):
    fandom = quote_plus(row["tag"])
    fandom_path = Path(f"{work_path}/{row['hash']}")
    fandom_tags_path = Path(f"{tag_path}/{row['hash']}/tags.csv.gz")
    if not fandom_tags_path.exists():
        print(f"tags for fandom {fandom} not available")
        continue
    if fandom_path.exists() and len(list(fandom_path.glob("*.csv"))) == len(months_to_scrape):
        print(f"already scraped {row['tag']}")
        continue
    fandom_path.mkdir(parents=True, exist_ok=True)
    fandom_tags = pd.read_csv(f"{tag_path}/{row['hash']}/tags.csv.gz")
    fandom_tags = {
        row["tag"]: i for i, row in fandom_tags.iterrows()
    }
    general_tags = pd.read_csv(f"{tag_path}/_general_tags.csv.gz")
    general_tags = {
        row["tag"]: i for i, row in general_tags.iterrows()
    }
    for months_ago in tqdm(range(months_ago_end, months_ago_start + 1), desc=fandom):
        csv_path = Path(f"{fandom_path}/{months_to_scrape[months_ago]}.csv")
        if csv_path.exists():
            print(f"already scraped {fandom}, {months_ago} months ago")
            continue
        url = work_url.format(months_ago=months_ago, fandom=fandom)
        response = scrape_url(url)
        soup = BeautifulSoup(response.text, "html.parser")
        # get number of pages (if there are any)
        pagination = soup.find("ol", class_="pagination actions")
        if pagination is None:
            print(f"no pages for {fandom}, {months_ago} months ago")
            results = get_works(soup, fandom_tags, general_tags)
            if results is None:
                print(f"no works for {fandom}, {months_ago} months ago")
                continue
            df = pd.DataFrame(
                results,
                columns=[
                    "id",
                    "title",
                    "author_hash",
                    "fandoms",
                    "fandom_tag_ids",
                    "general_tag_ids",
                    "language",
                    "words",
                    "chapters",
                    "comments",
                    "kudos",
                    "bookmarks",
                    "hits",
                    "date",
                    "rating",
                    "category",
                    "warnings",
                    "status",
                ],
            )
            df.to_csv(csv_path, index=False)
            continue
        last_page = int(pagination.find_all("li")[-2].text)
        print(f"found {last_page} pages for {fandom}, {months_ago} months ago")
        pages_to_scrape = []
        for page in range(1, last_page + 1):
            pages_to_scrape.append(url + f"&page={page}")
        results = []
        with ThreadPoolExecutor(max_workers=lco["num_threads"]) as executor:
            for result in tqdm(
                executor.map(scrape_page, pages_to_scrape),
                total=len(pages_to_scrape),
            ):
                if result is not None:
                    results.extend(result)
        if len(results) == 0:
            print(f"no works for {fandom}, {months_ago} months ago")
            continue
        df = pd.DataFrame(
            results,
            columns=[
                "id",
                "title",
                "author_hash",
                "fandoms",
                "fandom_tag_ids",
                "general_tag_ids",
                "language",
                "words",
                "chapters",
                "comments",
                "kudos",
                "bookmarks",
                "hits",
                "date",
                "rating",
                "category",
                "warnings",
                "status",
            ],
        )
        df.to_csv(csv_path, index=False)
                    