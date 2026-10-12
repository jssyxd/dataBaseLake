import os
import sys
import time
from huggingface_hub import snapshot_download

TOKEN = 'os.environ.get('HF_TOKEN', '')'
repo_id = 'mito0o852/OHLCV-1m'
target_dir = 'W:/数据库3/huggingface/mito0o852_OHLCV-1m'

print(f"Resuming download for {repo_id}...")
for attempt in range(1, 20):
    try:
        print(f"Attempt {attempt} at {time.strftime('%H:%M:%S')}...")
        snapshot_download(
            repo_id=repo_id,
            repo_type='dataset',
            local_dir=target_dir,
            token=TOKEN,
            max_workers=8
        )
        print("Successfully finished mito0o852/OHLCV-1m!")
        break
    except Exception as e:
        print(f"Attempt {attempt} failed: {e}")
        time.sleep(10)
