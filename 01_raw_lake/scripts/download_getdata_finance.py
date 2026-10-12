import os
import urllib.request
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

TOKEN = 'os.environ.get('GITHUB_TOKEN', '')'
BASE_DIR = 'W:/数据库3/github/getdata-finance'
os.makedirs(BASE_DIR, exist_ok=True)

headers = {
    'Authorization': f'Bearer {TOKEN}',
    'User-Agent': 'python-downloader'
}

print("Fetching all repositories for getdata-finance...")
all_repos = []
page = 1
while True:
    url = f'https://api.github.com/users/getdata-finance/repos?per_page=100&page={page}'
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            items = json.loads(resp.read().decode())
            if not items:
                break
            all_repos.extend(items)
            if len(items) < 100:
                break
            page += 1
    except Exception as e:
        print(f"Error fetching repos page {page}: {e}")
        break

print(f"Total repositories found: {len(all_repos)}")

download_tasks = []

def inspect_repo(repo_meta):
    name = repo_meta['name']
    url = f"https://api.github.com/repos/getdata-finance/{name}/contents"
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            files = json.loads(resp.read().decode())
            data_files = [f for f in files if f.get('type') == 'file' and (
                f['name'].lower().endswith('.csv') or 
                f['name'].lower().endswith('.parquet') or 
                f['name'].lower().endswith('.zip') or
                f['name'].lower().endswith('.gz')
            )]
            repo_dir = os.path.join(BASE_DIR, name)
            for df in data_files:
                target = os.path.join(repo_dir, df['name'])
                download_tasks.append((df['download_url'], target, df.get('size', 0), name))
    except Exception as e:
        print(f"Error inspecting {name}: {e}")

print("Inspecting repository contents...")
with ThreadPoolExecutor(max_workers=16) as ex:
    list(ex.map(inspect_repo, all_repos))

print(f"Found {len(download_tasks)} data files to download across {len(all_repos)} repos.")

def download_file(task):
    url, path, expected_size, repo_name = task
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return True, "exists", os.path.getsize(path)
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
            with open(path, 'wb') as f:
                f.write(data)
            return True, "ok", len(data)
    except Exception as e:
        return False, str(e), 0

success = 0
skipped = 0
failed = 0
total_size = 0

with ThreadPoolExecutor(max_workers=16) as ex:
    futures = {ex.submit(download_file, t): t for t in download_tasks}
    for idx, fut in enumerate(as_completed(futures), 1):
        ok, reason, sz = fut.result()
        if ok:
            if reason == "exists":
                skipped += 1
            else:
                success += 1
            total_size += sz
        else:
            failed += 1
        if idx % 50 == 0 or idx == len(download_tasks):
            print(f"Progress: [{idx}/{len(download_tasks)}] (New: {success}, Skipped: {skipped}, Failed: {failed}) - {total_size/(1024**2):.1f} MB")

print("Finished getdata-finance download!")
print(f"Summary: {success} new, {skipped} skipped, {failed} failed, Total size: {total_size/(1024**2):.2f} MB")
