import pandas as pd
import lco
from rich import print

lco.init("config.yml")

df = pd.read_csv(f"{lco['process_dir']}/canonical_fandoms.csv")

count_above_100 = df[df["count"] >= 120_000].shape[0]
pct_above_100 = count_above_100 / df.shape[0]
sum_above_100 = df[df["count"] >= 120_000]["count"].sum()
pct_sum_above_100 = sum_above_100 / df["count"].sum()

print(f"found {count_above_100} fandoms with 120,000 or more works")
print(f"that's {pct_above_100 * 100:.2f}% of all fandoms")
print(f"and {sum_above_100} works, or {pct_sum_above_100 * 100:.2f}% of all works")

# harry potter tags
df = pd.read_csv(f"{lco['process_dir']}/tags/victor-east-jupiter-xray/tags.csv.gz")

count_above_10 = df[df["count"] >= 10].shape[0]
pct_above_10 = count_above_10 / df.shape[0]

print()
print(f"found {count_above_10} tags with 10 or more works in the harry potter fandom")
print(f"that's {pct_above_10 * 100:.2f}% of all tags")