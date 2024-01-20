import os

bucket = 'vincent-li'
tables = os.environ.get("TABLES", "defi.cheating_wallet").split(",")
lark_url = os.environ.get("LARK_URL", "")
update_interval = int(os.environ.get("UPDATE_INTERVAL_MINUTES", 60))
uploading = int(float(os.environ.get("uploading", 0.0)))