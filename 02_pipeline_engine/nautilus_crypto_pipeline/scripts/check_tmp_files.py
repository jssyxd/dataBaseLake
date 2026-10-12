from pathlib import Path
tmp = Path("/tmp/nautilus_test_catalog")
print(list(tmp.glob("**/*")))
