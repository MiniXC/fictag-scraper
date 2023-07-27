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

work_path = Path(f"{lco['data_dir']}/works")

if not work_path.exists():
    work_path.mkdir(parents=True)

fandom_df = pd.read_csv(f"{lco['process_dir']}/canonical_fandoms.csv")

work_url = "https://archiveofourown.org/works/search?work_search%5Bquery%5D=&work_search%5Btitle%5D=&work_search%5Bcreators%5D=&work_search%5Brevised_at%5D={years_ago}+years+ago&work_search%5Bcomplete%5D=&work_search%5Bcrossover%5D=&work_search%5Bsingle_chapter%5D=0&work_search%5Bword_count%5D=&work_search%5Blanguage_id%5D=&work_search%5Bfandom_names%5D={fandom}&work_search%5Brating_ids%5D=&work_search%5Bcharacter_names%5D=&work_search%5Brelationship_names%5D=&work_search%5Bfreeform_names%5D=&work_search%5Bhits%5D=&work_search%5Bkudos_count%5D=&work_search%5Bcomments_count%5D=&work_search%5Bbookmarks_count%5D=&work_search%5Dsort_column%5D=revised_at&work_search%5Dsort_direction%5D=asc&commit=Search"

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

enc_relationships = {
    "F/F": "F",
    "F/M": "H",
    "Gen": "G",
    "M/M": "M",
    "Multi": "U",
    "No category": "0",
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
    works = soup.find_all("li", class_="work blurb group")
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
        relationships = work.find("span", class_="category").text
        if "," in relationships:
            relationships = relationships.split(",")
            relationships = [r.strip() for r in relationships]
        else:
            relationships = [relationships]
        relationships = [enc_relationships[r] for r in relationships]
        relationships = "".join(relationships)
        warnings = work.find("span", class_="warnings").text
        if "," in warnings:
            warnings = warnings.split(",")
            warnings = [w.strip() for w in warnings]
        else:
            warnings = [warnings]
        warnings = [enc_warnings[w] for w in warnings]
        warnings = "".join(warnings)
        status = work.find("span", class_="words").text
        status = status.strip()
        status = enc_status[status]
        heading = work.find("h4", class_="heading")
        # first <a> tag is title, second is author
        title = heading.find_all("a")[0].text
        author = heading.find_all("a")[1].text
        # hash author
        author_hash = hashlib.sha256(author.encode("utf-8")).hexdigest()
        # work id is in the href
        work_id = int(heading.find_all("a")[0]["href"].split("/")[-1])
        # other fandoms
        fandoms = work.find("h5", class_="fandoms heading").find_all("a")
        if len(fandoms) > 1:
            fandoms = fandoms[1:]
        else:
            fandoms = []
        fandoms = [f.text for f in fandoms]
        # 

    return results