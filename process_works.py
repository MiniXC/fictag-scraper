import pandas as pd
from pathlib import Path
from tqdm.auto import tqdm
import lco

lco.init("config.yml")

fandom_df = pd.read_csv(f"{lco['process_dir']}/canonical_fandoms.csv