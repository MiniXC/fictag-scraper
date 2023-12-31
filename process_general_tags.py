import pandas as pd
import lco
from pathlib import Path

lco.init("config.yml")

data_dir = lco["data_dir"]
process_dir = lco["process_dir"]

file_path = Path(f"{data_dir}/tags/_general_tags.csv")

df = pd.read_csv(file_path)

df = df.sort_values("count", ascending=False)

Path(f"{process_dir}/tags").mkdir(parents=True, exist_ok=True)

# we need the index to later join with the works
df.reset_index(inplace=True)
df.to_csv(f"{process_dir}/tags/_general_tags.csv.gz", index=True)
