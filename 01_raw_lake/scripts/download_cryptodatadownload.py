import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_DIR = 'W:/数据库3/cryptodatadownload'
os.makedirs(BASE_DIR, exist_ok=True)

exchanges = ['binance', 'bitstamp', 'bitfinex', 'gemini', 'poloniex', 'deribit', 'kucoin']
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=3)
session.mount('https://', adapter)
session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})

tasks = []
print("Collecting available files from CryptoDataDownload API...")
for ex in exchanges:
    ex_dir = os.path.join(BASE_DIR, ex)
    os.makedirs(ex_dir, exist_ok=True)
    api_url = f"https://api.cryptodatadownload.com/v1/data/ohlc/{ex}/available"
    try:
        r = session.get(api_url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            items = data.get('data', [])
            for it in items:
                file_url = it.get('file')
                if file_url:
                    fname = os.path.basename(file_url)
                    target_path = os.path.join(ex_dir, fname)
                    tasks.append((file_url, target_path, ex, fname))
            print(f"  {ex}: {len(items)} files listed")
    except Exception as e:
        print(f"  {ex}: error {e}")

print(f"Total files to download: {len(tasks)}")

def download_one(task):
    url, path, ex, fname = task
    if os.path.exists(path) and os.path.getsize(path) > 100:
        return True, "exists", os.path.getsize(path)
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 200 and len(resp.content) > 50:
            with open(path, 'wb') as f:
                f.write(resp.content)
            return True, "ok", len(resp.content)
        else:
            return False, f"status {resp.status_code}", 0
    except Exception as e:
        return False, str(e), 0

success_count = 0
skip_count = 0
fail_count = 0
total_bytes = 0

with ThreadPoolExecutor(max_workers=32) as executor:
    futures = {executor.submit(download_one, t): t for t in tasks}
    for idx, fut in enumerate(as_completed(futures), 1):
        ok, reason, sz = fut.result()
        if ok:
            if reason == "exists":
                skip_count += 1
            else:
                success_count += 1
            total_bytes += sz
        else:
            fail_count += 1
        if idx % 200 == 0 or idx == len(tasks):
            print(f"Progress: [{idx}/{len(tasks)}] (New: {success_count}, Existing: {skip_count}, Failed: {fail_count}) - {total_bytes / (1024*1024):.1f} MB")

print("CryptoDataDownload finished!")
print(f"Summary: {success_count} downloaded, {skip_count} skipped, {fail_count} failed, Total size: {total_bytes/(1024**2):.2f} MB")
