import sys
import os
from pathlib import Path

ROOT = Path('W:/数据库3/github/cenkcorapci_download-lots-of-financial-data')
sys.path.insert(0, str(ROOT))

from scripts.downloaders.company import COMPANY_DOWNLOADERS
from scripts.downloaders.crypto import CRYPTO_DOWNLOADERS
from scripts.downloaders.macro import MACRO_DOWNLOADERS
from scripts.downloaders.news import NEWS_DOWNLOADERS
from scripts.downloaders.polymarket import POLYMARKET_DOWNLOADERS
from scripts.downloaders.kaggle_hf import KAGGLE_HF_DOWNLOADERS

os.environ['KAGGLE_API_TOKEN'] = 'KGAT_ac40707589c873af896d44997f6c0d40'
os.environ['HF_TOKEN'] = 'os.environ.get('HF_TOKEN', '')'

downloaders = (
    CRYPTO_DOWNLOADERS +
    POLYMARKET_DOWNLOADERS +
    NEWS_DOWNLOADERS +
    MACRO_DOWNLOADERS +
    COMPANY_DOWNLOADERS +
    KAGGLE_HF_DOWNLOADERS
)

print(f"Running {len(downloaders)} downloaders from cenkcorapci...")
results = []
for cls in downloaders:
    dl = cls()
    print(f"\n--- Running: {dl.name} ({dl.category}) ---")
    try:
        res = dl.run()
        print(f"  Result: success={res.success}, rows={res.rows}, files={len(res.files)}, error={res.error}")
        results.append(res)
    except Exception as e:
        print(f"  Exception: {e}")

print(f"\nCompleted {len(results)} downloaders!")
