import pandas as pd
import lco
from pathlib import Path
from tqdm.auto import tqdm

lco.init("config.yml")

data_dir = lco["data_dir"]
process_dir = lco["process_dir"]

fandom_df = pd.read_csv(f"{process_dir}/canonical_fandoms.csv")
fandom_df = fandom_df[fandom_df["count"] >= 100]

for _, row in tqdm(fandom_df.iterrows(), total=len(fandom_df)):
    fandom_path = Path(f"{data_dir}/tags/{row['hash']}/tags.csv")
    fandom_path_new = Path(f"{process_dir}/tags/{row['hash']}/tags.csv.gz")
    if fandom_path_new.exists():
        print(f"already processed {row['tag']}")
        continue
    if not fandom_path.exists():
        print(f"no tags for {row['tag']}")
        continue
    if fandom_path.stat().st_size == 0:
        print(f"empty file for {row['tag']}")
        continue
    df = pd.read_csv(fandom_path)
    df = df.sort_values("count", ascending=False)
    df.reset_index(inplace=True)
    fandom_path_new.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(fandom_path_new, index=True)