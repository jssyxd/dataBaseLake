#!/usr/bin/env python3
"""
Zero-Configuration Reusable Quant Database Loader for All Agents
Directly supports Polars LazyFrame, Pandas, and DuckDB over local cache and NFS
Location: nfs://fnos2.local/fs/1000/nfs/iflow/database/
"""
import os
import glob
import subprocess
import logging
from pathlib import Path
from typing import Optional, Union, List
import polars as pl

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("QuantDataLoader")

DEFAULT_NFS_URL = "nfs://fnos2.local/fs/1000/nfs/iflow/database"
DEFAULT_CACHE_DIR = os.path.expanduser("~/.cache/quant_db")

class QuantDataLoader:
    def __init__(self, cache_dir: str = DEFAULT_CACHE_DIR, nfs_url: str = DEFAULT_NFS_URL):
        self.cache_dir = Path(cache_dir)
        self.nfs_url = nfs_url.rstrip("/")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
    def _sync_from_nfs(self, rel_path: str) -> Path:
        """
        Ensure file is in local cache, fetching via nfs-cp if missing.
        """
        local_file = self.cache_dir / rel_path
        if local_file.exists():
            return local_file
            
        local_file.parent.mkdir(parents=True, exist_ok=True)
        remote_url = f"{self.nfs_url}/{rel_path.lstrip('/')}"
        logger.info(f"Syncing from NFS: {remote_url} -> {local_file}")
        cmd = ["nfs-cp", remote_url, str(local_file)]
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to fetch {remote_url}: {e.stderr}")
            raise FileNotFoundError(f"Asset not found in database: {rel_path}")
            
        return local_file

    def load(
        self,
        asset: str,
        timeframe: str = "1d",
        start: Optional[str] = None,
        end: Optional[str] = None,
        as_format: str = "polars"
    ) -> Union[pl.DataFrame, object]:
        """
        Load backtest dataset for any symbol and timeframe.
        
        Args:
            asset: e.g. "us_equities/SPY", "us_futures/ES", "crypto/futures_um/BTCUSDT"
            timeframe: "1m", "15m", "1h", "4h", "1d"

        Returns:
            Clean standardized OHLCV DataFrame
        """
        # Formulate relative path
        symbol_name = asset.split("/")[-1]
        
        # Check standard file naming patterns
        candidates = [
            f"{asset}/{timeframe}/{symbol_name}_{timeframe}_consolidated.parquet",
            f"{asset}/{timeframe}/{symbol_name}_{timeframe}.parquet"
        ]
        
        local_path = None
        for cand in candidates:
            try:
                local_path = self._sync_from_nfs(cand)
                break
            except Exception:
                continue
                
        if not local_path or not local_path.exists():
            # Check if there are monthly shards
            raise FileNotFoundError(f"Could not locate dataset for {asset} [{timeframe}]")
            
        df = pl.read_parquet(local_path)
        
        # Apply date filters
        if start:
            if "datetime" in df.columns:
                df = df.filter(pl.col("datetime") >= start)
        if end:
            if "datetime" in df.columns:
                df = df.filter(pl.col("datetime") <= end)
        if as_format.lower() == "pandas":
            return df.to_pandas()
        return df

    def scan(self, asset: str, timeframe: str = "1d") -> pl.LazyFrame:
        """
        Return a Polars LazyFrame for zero-memory, predicate pushdown queries.
        """
        symbol_name = asset.split("/")[-1]
        candidates = [
            f"{asset}/{timeframe}/{symbol_name}_{timeframe}_consolidated.parquet",
            f"{asset}/{timeframe}/{symbol_name}_{timeframe}.parquet"
        ]
        local_path = None
        for cand in candidates:
            try:
                local_path = self._sync_from_nfs(cand)
                break
            except Exception:
                continue
        if not local_path:
            raise FileNotFoundError(f"Asset not found: {asset} [{timeframe}]")
        return pl.scan_parquet(local_path)

if __name__ == "__main__":
    db = QuantDataLoader()
    print("QuantDataLoader initialized successfully.")
