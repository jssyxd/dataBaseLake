import os
import sys
import time
from huggingface_hub import snapshot_download
os.environ['HF_XET_HIGH_PERFORMANCE'] = '1'

TOKEN = 'os.environ.get('HF_TOKEN', '')'
BASE_DIR = 'W:/数据库3/huggingface'
os.makedirs(BASE_DIR, exist_ok=True)

datasets = [
    ('123olp/binance-futures-ohlcv-2018-2026', '123olp_binance-futures-ohlcv-2018-2026'),
    ('OMCHOKSI108/my-cloud-data-lake', 'OMCHOKSI108_my-cloud-data-lake'),
    ('twelvedata/financial-world-model', 'twelvedata_financial-world-model'),
    ('mito0o852/OHLCV-1m', 'mito0o852_OHLCV-1m')
]

for repo_id, folder_name in datasets:
    target_dir = os.path.join(BASE_DIR, folder_name)
    print(f"\n==========================================")
    print(f"Starting download: {repo_id}")
    print(f"Destination: {target_dir}")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"==========================================")
    
    attempts = 0
    max_attempts = 5
    while attempts < max_attempts:
        attempts += 1
        try:
            start_t = time.time()
            snapshot_download(
                repo_id=repo_id,
                repo_type='dataset',
                local_dir=target_dir,
                token=TOKEN,
                max_workers=16
            )
            elapsed = time.time() - start_t
            # calculate total folder size
            total_size = sum(os.path.getsize(os.path.join(r, f)) for r, d, fs in os.walk(target_dir) for f in fs)
            print(f"Successfully completed {repo_id} in {elapsed:.1f}s ({total_size/(1024**3):.2f} GB)")
            break
        except Exception as e:
            print(f"Attempt {attempts} failed with error: {e}")
            if attempts < max_attempts:
                print("Retrying in 5 seconds...")
                time.sleep(5)
            else:
                print(f"Failed to download {repo_id} after {max_attempts} attempts.")

print("\nAll target Hugging Face parquet/large datasets processed!")
