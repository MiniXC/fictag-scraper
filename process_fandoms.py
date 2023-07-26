import pandas as pd
import lco
from humanhash import humanize
import hashlib
from tqdm.auto import tqdm

lco.init("config.yml")

data_dir = lco["data_dir"]
process_dir = lco["process_dir"]

df = pd.read_csv(f"{data_dir}/canonical_fandoms.csv")

df = df.sort_values("count", ascending=False)

new_hash = []

for i, row in tqdm(df.iterrows()):
    digest = hashlib.sha256(row["href"].encode()).hexdigest()
    tag_hash = humanize(digest, words=4)
    new_hash.append(tag_hash)

df["hash"] = new_hash

df.to_csv(f"{process_dir}/canonical_fandoms.csv", index=False)