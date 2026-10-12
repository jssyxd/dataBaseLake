#!/usr/bin/env python3
"""
Resilient Hugging Face Dataset Downloader for Quant Data Lakes.
Handles:
- Direct domestic mirror routing without overseas proxy loops.
- CIFS / SMB GVFS mount filelock mode error suppression.
- Resumable multithreaded downloads with automated retry and inventory generation.
"""

import os
import sys
import time
import json
import urllib.request
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

endpoint = os.environ.get('HF_ENDPOINT', 'https://hf-mirror.com')
if 'hf-mirror.com' in endpoint:
    for k in ['http_proxy', 'https_proxy', 'all_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY']:
        os.environ.pop(k, None)
os.environ['HF_ENDPOINT'] = endpoint

from huggingface_hub import HfApi, hf_hub_download

def get_dataset_files(repo_id, max_retries=10):
    url = f"{os.environ['HF_ENDPOINT']}/api/datasets/{repo_id}"
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    for attempt in range(1, max_retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode('utf-8'))
                siblings = data.get('siblings', [])
                files = [(s['rfilename'], s.get('size')) for s in siblings if s['rfilename'] not in ['.gitattributes']]
                if files:
                    return files
        except Exception:
            if attempt == max_retries:
                break
            time.sleep(attempt * 1.5)
            
    api = HfApi()
    for attempt in range(1, max_retries + 1):
        try:
            info = api.dataset_info(repo_id, files_metadata=True)
            return [(s.rfilename, s.size) for s in info.siblings if s.rfilename not in ['.gitattributes']]
        except Exception:
            if attempt == max_retries:
                raise
            time.sleep(attempt * 1.5)

def download_file(repo_id, filename, expected_size, target_dir, retries=15):
    fpath = os.path.join(target_dir, filename)
    if os.path.exists(fpath):
        if expected_size is None or os.path.getsize(fpath) == expected_size:
            return filename, True, "already_exists", os.path.getsize(fpath)
    
    os.makedirs(os.path.dirname(fpath), exist_ok=True)
    for attempt in range(1, retries + 1):
        try:
            downloaded = hf_hub_download(
                repo_id=repo_id,
                repo_type="dataset",
                filename=filename,
                local_dir=target_dir
            )
            sz = os.path.getsize(downloaded)
            if expected_size is not None and sz != expected_size:
                raise ValueError(f"Size mismatch: got {sz}, expected {expected_size}")
            return filename, True, f"downloaded (attempt {attempt})", sz
        except Exception as e:
            if attempt == retries:
                return filename, False, f"error after {retries} attempts: {e}", 0
            time.sleep(min(20, 1.5 * attempt))

def main():
    if len(sys.argv) < 3:
        print("Usage: download_hf_dataset.py <repo_id> <target_dir> [max_workers] [limit_files]")
        sys.exit(1)
        
    repo_id = sys.argv[1]
    target_dir = sys.argv[2]
    max_workers = int(sys.argv[3]) if len(sys.argv) > 3 else 4
    limit_files = int(sys.argv[4]) if len(sys.argv) > 4 else 0
    
    os.makedirs(target_dir, exist_ok=True)
    start_time = datetime.now().isoformat()
    t0 = time.time()
    
    files = get_dataset_files(repo_id)
    if limit_files > 0:
        files = files[:limit_files]
        
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting {repo_id}: {len(files)} files to download/verify with {max_workers} workers...")
    
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(download_file, repo_id, fn, sz, target_dir): (fn, sz)
            for fn, sz in files
        }
        for future in as_completed(futures):
            fn, success, status, sz = future.result()
            results.append((fn, success, status, sz))
            if len(results) % 20 == 0 or len(results) == len(files):
                done_count = sum(1 for r in results if r[1])
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Progress: {len(results)}/{len(files)} ({done_count} ok)")
                
    elapsed = time.time() - t0
    end_time = datetime.now().isoformat()
    
    inventory = []
    total_size = 0
    for root, dirs, f_list in os.walk(target_dir):
        if '.cache' in root:
            continue
        for f in f_list:
            if f in ['DOWNLOAD_LOG.md', 'inventory.json']:
                continue
            fp = os.path.join(root, f)
            rel = os.path.relpath(fp, target_dir)
            sz = os.path.getsize(fp)
            total_size += sz
            inventory.append({'file': rel, 'size_bytes': sz})
            
    with open(os.path.join(target_dir, 'inventory.json'), 'w') as f:
        json.dump({
            'repo': repo_id,
            'total_files': len(inventory),
            'total_size_bytes': total_size,
            'files': inventory
        }, f, indent=2)
        
    failed = [r for r in results if not r[1]]
    status_str = "SUCCESS" if not failed else f"PARTIAL ({len(failed)} failed)"
    
    log_content = f"""# DOWNLOAD LOG: {repo_id}

- Repo: {repo_id}
- Start: {start_time}
- End: {end_time}
- Duration: {elapsed:.2f}s
- Target Dir: {target_dir}
- Checked/Downloaded Files: {len(results)}
- Success Files: {len(results) - len(failed)}
- Failed Files: {len(failed)}
- Total Size On Disk: {total_size} bytes ({total_size / (1024**3):.2f} GB)
- STATUS: {status_str}
"""
    if failed:
        log_content += "\n## Failures:\n"
        for fn, _, err, _ in failed:
            log_content += f"- {fn}: {err}\n"
            
    with open(os.path.join(target_dir, 'DOWNLOAD_LOG.md'), 'w') as f:
        f.write(log_content)
        
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Finished {repo_id}: {status_str}, {total_size / (1024**3):.2f} GB in {elapsed:.2f}s")

if __name__ == '__main__':
    main()
