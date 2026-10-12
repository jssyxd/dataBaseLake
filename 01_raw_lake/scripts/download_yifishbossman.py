import asyncio
import aiohttp
import os
import time
import zipfile
from huggingface_hub import HfApi

TOKEN = 'os.environ.get('HF_TOKEN', '')'
BASE_DIR = 'W:/数据库3/huggingface/yifishbossman'
os.makedirs(BASE_DIR, exist_ok=True)

api = HfApi(token=TOKEN)

targets = [
    ('yifishbossman/financial-analyst-data-demo', 'financial-analyst-data-demo.zip'),
    ('yifishbossman/financial-analyst-data-lite', 'financial-analyst-data-lite.zip'),
    ('yifishbossman/financial-analyst-data-full', 'financial-analyst-data-full.zip')
]

async def download_repo_to_zip(repo_id, zip_filename):
    zip_path = os.path.join(BASE_DIR, zip_filename)
    if os.path.exists(zip_path) and os.path.getsize(zip_path) > 10 * 1024 * 1024:
        print(f"Already exists: {zip_filename} ({os.path.getsize(zip_path)/(1024**2):.1f} MB), skipping.")
        return

    print(f"\nFetching file list for {repo_id}...")
    all_files = list(api.list_repo_files(repo_id, repo_type='dataset'))
    files = [f for f in all_files if not f.startswith('.git')]
    print(f"Total files to fetch: {len(files)}")

    headers = {'Authorization': f'Bearer {TOKEN}'}
    connector = aiohttp.TCPConnector(limit=64)
    timeout = aiohttp.ClientTimeout(total=60)

    start_time = time.time()
    zf = zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED)
    
    downloaded_count = 0
    total_bytes = 0
    failed_files = []

    sem = asyncio.Semaphore(64)

    async with aiohttp.ClientSession(connector=connector, headers=headers, timeout=timeout) as session:
        async def fetch(file_path):
            nonlocal downloaded_count, total_bytes
            url = f"https://huggingface.co/datasets/{repo_id}/resolve/main/{file_path}"
            for retry in range(3):
                try:
                    async with sem:
                        async with session.get(url) as resp:
                            if resp.status == 200:
                                data = await resp.read()
                                return file_path, data
                except Exception:
                    await asyncio.sleep(1)
            return file_path, None

        tasks = [asyncio.create_task(fetch(f)) for f in files]
        
        for idx, fut in enumerate(asyncio.as_completed(tasks), 1):
            fpath, data = await fut
            if data is not None:
                zf.writestr(fpath, data)
                downloaded_count += 1
                total_bytes += len(data)
            else:
                failed_files.append(fpath)

            if idx % 1000 == 0 or idx == len(files):
                rate = downloaded_count / max(1, (time.time() - start_time))
                print(f"[{idx}/{len(files)}] {zip_filename}: {downloaded_count} saved, {len(failed_files)} failed ({rate:.1f} files/s) - {total_bytes/(1024**2):.1f} MB")

    zf.close()
    elapsed = time.time() - start_time
    print(f"Completed {zip_filename} in {elapsed:.1f}s: {downloaded_count} files, {os.path.getsize(zip_path)/(1024**2):.2f} MB")

async def main():
    for repo_id, zip_name in targets:
        await download_repo_to_zip(repo_id, zip_name)

if __name__ == '__main__':
    asyncio.run(main())
