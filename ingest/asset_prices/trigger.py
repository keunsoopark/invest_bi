import requests
import datetime
import time

API_URL = "https://europe-north1-xnwk-462111.cloudfunctions.net/asset_prices_daily"

# Date range
start_date = datetime.date(2026, 2, 20)
end_date = datetime.date(2026, 3, 4)
# asset_ids = ["364970.KS", "364980.KS"]
asset_ids = ["0P0000K20Z.IR", "0P00000GP8.IR", "0P0000HNUP.IR", "0P00018J0O.IR", "0P00016TML.IR", "0P00000MY5.IR", "0P00000O8G.IR", "0P00017YPU.IR", "0P0000TJ5D.IR", "0P0001BWAR.IR"]

# Loop through each date
current_date = start_date
last_successful_date = None

while current_date <= end_date:
    payload = {"date": current_date.isoformat()}

    if "asset_ids" in locals():
        payload["asset_ids"] = asset_ids
    print(f"Triggering API for {payload['date']}...")

    try:
        response = requests.post(API_URL, json=payload)
        if response.status_code == 200:
            print(f"✅ Success: {current_date}")
            print(f"From API: {response.text}")
            last_successful_date = current_date
        else:
            print(f"❌ Failed at {current_date} with status code: {response.status_code}")
            break
    except Exception as e:
        print(f"❌ Exception occurred on {current_date}: {e}")
        break

    # Wait a short time between requests (optional)
    time.sleep(1)

    current_date += datetime.timedelta(days=1)

if last_successful_date != end_date:
    print(f"\nStopped. Last successful date: {last_successful_date}")
else:
    print("\n✅ All dates processed successfully.")
