#!/usr/bin/env python3
"""
Centralized Data Lake Bootstrap & Restoration Tool (dataBaseLake)
-----------------------------------------------------------------
Supports zero-configuration one-command data bootstrapping on any new machine.
Allows full restoration, tier-based restoration, or granular dataset retrieval
with automatic resume, domestic mirror routing, and integrity checks.
"""

import os
import sys
import json
import argparse
import subprocess
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
REGISTRY_FILE = BASE_DIR / "data_sources.json"


def load_registry():
    if not REGISTRY_FILE.exists():
        print(f"[ERROR] Registry file {REGISTRY_FILE} not found!")
        sys.exit(1)
    with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def print_status(registry):
    print("=" * 80)
    print(f"dataBaseLake Status & Integrity Report ({registry.get('version', '1.0.0')})")
    print("=" * 80)
    total_local_size = 0
    total_expected_size = 0

    for ds in registry.get("datasets", []):
        ds_id = ds["id"]
        rel_target = ds["local_target_dir"]
        abs_target = BASE_DIR / rel_target
        exp_gb = ds.get("size_approx_gb", 0)
        total_expected_size += exp_gb

        exists = abs_target.exists()
        local_size_mb = 0
        file_count = 0
        if exists:
            for root, _, files in os.walk(abs_target):
                file_count += len(files)
                for f in files:
                    try:
                        local_size_mb += os.path.getsize(os.path.join(root, f)) / (1024 * 1024)
                    except:
                        pass
        local_gb = local_size_mb / 1024
        total_local_size += local_gb

        status_tag = "[INSTALLED]" if local_gb >= (exp_gb * 0.7) else ("[PARTIAL]" if local_gb > 0 else "[MISSING]")
        print(f"{status_tag:11s} {ds_id:25s} | Local: {local_gb:6.2f} GB ({file_count:5d} files) | Expected: ~{exp_gb:5.1f} GB")

    print("-" * 80)
    print(f"Summary: Local Storage: {total_local_size:6.2f} GB / Registered Expected: ~{total_expected_size:6.1f} GB")
    print("=" * 80)


def download_huggingface(repo_id, target_dir, use_mirror=True):
    print(f"\n>>> [HuggingFace] Restoring {repo_id} -> {target_dir}")
    os.makedirs(target_dir, exist_ok=True)
    env = os.environ.copy()
    if use_mirror:
        env["HF_ENDPOINT"] = "https://hf-mirror.com"
        print("  -> Using China Domestic Mirror: https://hf-mirror.com")
    env["HF_HUB_ENABLE_HF_TRANSFER"] = "1"

    # Prefer python huggingface_hub snapshot_download
    code = f"""
import os
from huggingface_hub import snapshot_download
repo_id = "{repo_id}"
local_dir = "{target_dir.as_posix()}"
print(f"Starting snapshot_download for {{repo_id}}...")
snapshot_download(
    repo_id=repo_id,
    repo_type="dataset",
    local_dir=local_dir,
    resume_download=True,
    max_workers=8
)
print("Download completed successfully!")
"""
    cmd = [sys.executable, "-c", code]
    res = subprocess.run(cmd, env=env)
    return res.returncode == 0


def download_cdd(target_dir):
    script_path = BASE_DIR / "01_raw_lake" / "scripts" / "download_cryptodatadownload.py"
    if script_path.exists():
        print(f"\n>>> [CryptoDataDownload] Executing {script_path} -> {target_dir}")
        res = subprocess.run([sys.executable, str(script_path)])
        return res.returncode == 0
    else:
        print(f"[ERROR] Script {script_path} not found.")
        return False


def download_github_getdata(target_dir):
    script_path = BASE_DIR / "01_raw_lake" / "scripts" / "download_getdata_finance.py"
    if script_path.exists():
        print(f"\n>>> [GitHub GetData] Executing {script_path} -> {target_dir}")
        res = subprocess.run([sys.executable, str(script_path)])
        return res.returncode == 0
    else:
        print(f"[ERROR] Script {script_path} not found.")
        return False


def restore_dataset(ds, use_mirror=True):
    ds_id = ds["id"]
    target_dir = BASE_DIR / ds["local_target_dir"]
    stype = ds.get("source_type")

    print(f"\n========================================================")
    print(f"Starting restoration for dataset: {ds_id} ({ds.get('description')})")
    print(f"Target path: {target_dir}")
    print(f"========================================================")

    if stype == "huggingface":
        return download_huggingface(ds["repo_id"], target_dir, use_mirror)
    elif stype == "direct_web" and "cryptodatadownload" in ds_id:
        return download_cdd(target_dir)
    elif stype == "github" and "getdata" in ds_id:
        return download_github_getdata(target_dir)
    elif stype == "kaggle":
        print(f"Kaggle dataset: {ds.get('kaggle_dataset')}")
        print("Run: kaggle datasets download -d " + ds.get("kaggle_dataset") + f" -p {target_dir} --unzip")
        return True
    else:
        print(f"[WARN] Unknown source type {stype} for dataset {ds_id}")
        return False


def main():
    parser = argparse.ArgumentParser(description="dataBaseLake Bootstrap & Restoration Manager")
    parser.add_argument("--status", action="store_true", help="Inspect local lake integrity and installed dataset stats")
    parser.add_argument("--list", action="store_true", help="List all available datasets in registry")
    parser.add_argument("--dataset", type=str, help="Download/restore specific dataset by ID (e.g. hf_mito0o852_ohlcv1m)")
    parser.add_argument("--tier", type=str, choices=["01_raw_lake", "03_governed_lake"], help="Restore all datasets in a given tier")
    parser.add_argument("--all", action="store_true", help="Restore all registered datasets")
    parser.add_argument("--no-mirror", action="store_true", help="Use official huggingface.co instead of domestic mirror")

    args = parser.parse_args()
    registry = load_registry()

    if args.status or (not any([args.status, args.list, args.dataset, args.tier, args.all])):
        print_status(registry)
        return

    if args.list:
        print(f"\nAvailable datasets in dataBaseLake registry ({len(registry['datasets'])} items):")
        for ds in registry["datasets"]:
            print(f" - {ds['id']:28s} [{ds['tier']}] ~{ds.get('size_approx_gb', 0)} GB : {ds['description']}")
        return

    use_mirror = not args.no_mirror

    if args.dataset:
        matched = [d for d in registry["datasets"] if d["id"] == args.dataset]
        if not matched:
            print(f"[ERROR] Dataset id '{args.dataset}' not found in data_sources.json")
            sys.exit(1)
        restore_dataset(matched[0], use_mirror)
        return

    if args.tier:
        matched = [d for d in registry["datasets"] if d["tier"] == args.tier]
        for ds in matched:
            restore_dataset(ds, use_mirror)
        return

    if args.all:
        print("Restoring all datasets...")
        for ds in registry["datasets"]:
            restore_dataset(ds, use_mirror)


if __name__ == "__main__":
    main()
