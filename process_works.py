import pandas as pd
from pathlib import Path
from tqdm.auto import tqdm
import lco

lco.init("config.yml")

fandom_df = pd.read_csv(f"{lco['process_dir']}/canonical_fandoms.csv")
fandom_df = fandom_df[fandom_df["count"] >= 100]

general_tags = pd.read_csv(f"{lco['process_dir']}/tags/_general_tags.csv.gz")

for _, row in tqdm(fandom_df.iterrows(), total=len(fandom_df)):
    fandom = row["fandom"]
    fandom_hash = row["fandom_hash"]
    works_path = Path(f"{lco['data_dir']}/works/{fandom_hash}")
    tags_path = Path(f"{lco['process_dir']}/tags/{fandom_hash}/tags.csv.gz")
    if not tags_path.exists() or tags_path.stat().st_size == 0:
        print(f"tags for {fandom} not scraped")
        continue
    if not works_path.exists():
        print(f"works for {fandom} not scraped")
        continue
    tags_df = pd.read_csv(tags_path)
    for work_path in works_path.glob("*.csv.gz"):
        work_df = pd.read_csv(work_path)
        unknown_tags = {}
        for _, work_row in work_df.iterrows():
            # eval
            tags = eval(work_row["unknown_tags"])
            for tag in tags:
                if tag not in unknown_tags:
                    unknown_tags[tag] = 0
                unknown_tags[tag] += 1
        print(unknown_tags)
        raise
        
    