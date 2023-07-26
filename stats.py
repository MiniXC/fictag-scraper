import pandas as pd
import lco
from rich import print

lco.init("config.yml")

df = pd.read_csv(f"{lco['process_dir']}/canonical_fandoms.csv")

count_above_100 = df[df["count"] >= 100].shape[0]
pct_above_100 = count_above_100 / df.shape[0]

print(f"found {count_above_100} fandoms with 100 or more works")
print(f"that's {pct_above_100 * 100:.2f}% of all fandoms")