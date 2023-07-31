import pandas as pd
from pathlib import Path
from tqdm.auto import tqdm
from sentence_transformers import SentenceTransformer, util
import numpy as np
import lco
import torch
import argparse

general_tag_cache = {}
fandom_tag_cache = {}

parser = argparse.ArgumentParser()
parser.add_argument("--overwrite", action="store_true")
parser.add_argument("--fandom", type=str, default=None)

args = parser.parse_args()

lco.init("config.yml")

model = SentenceTransformer('all-MiniLM-L6-v2')

fandom_df = pd.read_csv(f"{lco['process_dir']}/canonical_fandoms.csv")
fandom_df = fandom_df[fandom_df["count"] >= 100]
if args.fandom is not None:
    fandom_df = fandom_df[fandom_df["tag"] == args.fandom]

general_tags = pd.read_csv(f"{lco['process_dir']}/tags/_general_tags.csv.gz")
general_tags = general_tags[general_tags["count"] >= 1]

if not Path(f"{lco['process_dir']}/tags/_general_tags_embeddings.npy").exists() or args.overwrite:
    general_tags_embeddings = model.encode(general_tags["tag"].tolist(), show_progress_bar=True)
    # save embeddings
    np.save(f"{lco['process_dir']}/tags/_general_tags_embeddings.npy", general_tags_embeddings)
else:
    general_tags_embeddings = np.load(f"{lco['process_dir']}/tags/_general_tags_embeddings.npy")

for _, row in tqdm(fandom_df.iterrows(), total=len(fandom_df)):
    fandom = row["tag"]
    fandom_hash = row["hash"]
    works_path = Path(f"{lco['data_dir']}/works/{fandom_hash}")
    processed_works_path = Path(f"{lco['process_dir']}/works/{fandom_hash}")
    if processed_works_path.exists() and len(list(processed_works_path.glob("*.csv.gz"))) > 0:
        continue
    if not processed_works_path.exists():
        processed_works_path.mkdir(parents=True)
    tags_path = Path(f"{lco['process_dir']}/tags/{fandom_hash}/tags.csv.gz")
    if not tags_path.exists() or tags_path.stat().st_size == 0:
        continue
    if not works_path.exists():
        continue
    tags_df = pd.read_csv(tags_path)
    tags_df = tags_df[tags_df["count"] >= 1]
    if not Path(f"{lco['process_dir']}/tags/{fandom_hash}/tags_embeddings.npy").exists() or args.overwrite:
        tags_embeddings = model.encode(tags_df["tag"].tolist(), show_progress_bar=True)
        # save embeddings
        np.save(f"{lco['process_dir']}/tags/{fandom_hash}/tags_embeddings.npy", tags_embeddings)
    else:
        tags_embeddings = np.load(f"{lco['process_dir']}/tags/{fandom_hash}/tags_embeddings.npy")
        work_glob = list(works_path.glob("*.csv"))
        work_add_general_tag_ids = []
        work_add_fandom_tag_ids = []
        for work_path in tqdm(work_glob, total=len(work_glob), desc=f'get unknown tags for {fandom}'):
            unknown_tags = {}
            work_df = pd.read_csv(work_path)
            for _, work_row in tqdm(work_df.iterrows(), total=len(work_df), desc=f'get unknown tags for {fandom}'):
                # eval
                tags = eval(work_row["unknown_tags"])
                if len(tags) == 0:
                    work_add_general_tag_ids.append("+")
                    work_add_fandom_tag_ids.append("+")
                    continue
                add_general_tag_ids = []
                add_fandom_tag_ids = []
                tags_to_remove = []
                for tag in tags:
                    if tag in general_tag_cache:
                        add_general_tag_ids.append(general_tag_cache[tag])
                        tags_to_remove.append(tag)
                    if tag in fandom_tag_cache:
                        add_fandom_tag_ids.append(fandom_tag_cache[tag])
                        tags_to_remove.append(tag)
                for tag in tags_to_remove:
                    tags.remove(tag)
                unknown_tags_embeddings = model.encode(tags)
                # compute cosine-similarity
                cos_scores = util.cos_sim(torch.tensor(unknown_tags_embeddings), torch.tensor(tags_embeddings))
                cos_scores = cos_scores.cpu().numpy()
                for i in range(len(tags)):
                    pair_candidate = {
                        "unknown_tag": str(tags[i]),
                        "tag": str(tags_df.iloc[np.argmax(cos_scores[i])]["tag"]),
                        "cos_score": np.max(cos_scores[i]),
                        "id": tags_df.iloc[np.argmax(cos_scores[i])]["index"],
                    }
                    if pair_candidate["cos_score"] >= 0.8:
                        # require the same number of "/" in the tag and the unknown tag
                        if pair_candidate["unknown_tag"].count("/") != pair_candidate["tag"].count("/"):
                            continue
                        # require the same number of "&" in the tag and the unknown tag
                        if pair_candidate["unknown_tag"].count("&") != pair_candidate["tag"].count("&"):
                            continue
                        # check if " " is in the tag but not in the unknown tag, and vice versa
                        if " " in pair_candidate["unknown_tag"] and " " not in pair_candidate["tag"]:
                            continue
                        if " " in pair_candidate["tag"] and " " not in pair_candidate["unknown_tag"]:
                            continue
                        fandom_tag_cache[pair_candidate["unknown_tag"]] = pair_candidate["id"]
                        add_fandom_tag_ids.append(pair_candidate["id"])
                cos_scores = util.pytorch_cos_sim(torch.tensor(unknown_tags_embeddings), torch.tensor(general_tags_embeddings))
                cos_scores = cos_scores.cpu().numpy()
                for i in range(len(tags)):
                    pair_candidate = {
                        "unknown_tag": str(tags[i]),
                        "tag": str(general_tags.iloc[np.argmax(cos_scores[i])]["tag"]),
                        "cos_score": np.max(cos_scores[i]),
                        "id": general_tags.iloc[np.argmax(cos_scores[i])]["index"],
                    }
                    if pair_candidate["cos_score"] >= 0.8:
                        # require the same number of "/" in the tag and the unknown tag
                        if pair_candidate["unknown_tag"].count("/") != pair_candidate["tag"].count("/"):
                            continue
                        # require the same number of "&" in the tag and the unknown tag
                        if pair_candidate["unknown_tag"].count("&") != pair_candidate["tag"].count("&"):
                            continue
                        # check if " " is in the tag but not in the unknown tag, and vice versa
                        if " " in pair_candidate["unknown_tag"] and " " not in pair_candidate["tag"]:
                            continue
                        if " " in pair_candidate["tag"] and " " not in pair_candidate["unknown_tag"]:
                            continue
                        general_tag_cache[pair_candidate["unknown_tag"]] = pair_candidate["id"]
                        add_general_tag_ids.append(pair_candidate["id"])
                work_add_general_tag_ids.append("+".join([str(i) for i in add_general_tag_ids]) + "+")
                work_add_fandom_tag_ids.append("+".join([str(i) for i in add_fandom_tag_ids]) + "+")
            work_df["computed_general_tag_ids"] = work_add_general_tag_ids
            work_df["computed_fandom_tag_ids"] = work_add_fandom_tag_ids
            work_df.to_csv(processed_works_path / work_path.name.replace(".csv", ".csv.gz"), index=False)
    