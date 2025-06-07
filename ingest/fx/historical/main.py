import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from google.cloud import bigquery

URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml"
PROJECT_ID = "xnwk-462111"
DATASET_NAME = "src_external"
TABLE_NAME = "fx"

today = datetime.utcnow().date()
min_date = today - timedelta(days=730)  # 2 years

response = requests.get(URL)
root = ET.fromstring(response.content)

rows = []
for time_cube in root.findall(".//{http://www.ecb.int/vocabulary/2002-08-01/eurofxref}Cube/{http://www.ecb.int/vocabulary/2002-08-01/eurofxref}Cube"):
    date_str = time_cube.attrib.get("time")
    if not date_str:
        continue

    parsed_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    if parsed_date < min_date:
        continue  # Skip older than 2 years

    rates = {
        c.attrib["currency"]: float(c.attrib["rate"])
        for c in time_cube.findall("{http://www.ecb.int/vocabulary/2002-08-01/eurofxref}Cube")
    }

    if "USD" in rates and "NOK" in rates and "KRW" in rates:
        usd_nok = (1 / rates["USD"]) * rates["NOK"]
        nok_krw = (1 / rates["NOK"]) * rates["KRW"]
        rows.append({
            "date": date_str,
            "USDNOK": usd_nok,
            "NOKKRW": nok_krw
        })

bq = bigquery.Client()
table_ref = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"

def batch_insert(client, table_ref, rows, batch_size=500):
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        errors = client.insert_rows_json(table_ref, batch)
        if errors:
            print(f"Insert errors in batch {i // batch_size}: {errors}")
        else:
            print(f"Inserted {len(batch)} rows in batch {i // batch_size}")

batch_insert(bq, table_ref, rows)
