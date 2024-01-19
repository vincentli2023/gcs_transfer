import os, json

bucket = 'vincent-li'
tables = os.environ.get("TABLES", "defi.all_label_address").split(",")
lark_url = os.environ.get("LARK_URL", "")
update_interval = int(os.environ.get("UPDATE_INTERVAL_MINUTES", 60))
uploading = int(os.environ.get("uploading", 0.0))