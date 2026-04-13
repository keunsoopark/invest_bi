import functions_framework
from google.cloud import bigquery
from datetime import datetime, timedelta
import yfinance as yf
import google.auth
import gspread
import pandas as pd
from google.auth.transport.requests import AuthorizedSession
import logging
import traceback
import sys
import gc

logger = logging.getLogger()
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(levelname)s: %(message)s"
)

GOOGLE_SHEETS_ID = "17ZHn4wq0Ga36_qEY6Qdt8jxzCnrSe48GRK-OvqUO1HQ"
SHEET_NAME = "assets"
PROJECT_ID = "xnwk-462111"
ASSET_PRICES_TABLE = f"{PROJECT_ID}.src_external.asset_prices"
ASSET_PRICES_TEMP_TABLE = f"{ASSET_PRICES_TABLE}_temp"
BATCH_SIZE = 20

ASSET_PRICES_SCHEMA = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("asset_name", "STRING"),
    bigquery.SchemaField("asset_id", "STRING"),
    bigquery.SchemaField("price", "FLOAT"),
    bigquery.SchemaField("currency", "STRING")
]

# Asset IDs with delayed data publish (e.g., KLP funds)
# These assets may not have D-1 data available in the morning, so we try D-2, D-3, etc.
DELAYED_ASSET_IDS = {
    "0P0000K20Z.IR",    # KLP STATSOBLIGASJON P
    "0P00000GP8.IR",    # KLP AKSJENORGE AKTIV P
    "0P0000HNUP.IR",    # KLP AKSJENORGE INDEKS P
    "0P00018J0O.IR",    # KLP AKSJEUSA INDEKS P
    "0P00016TML.IR",    # KLP AKSJEEUROPA INDEKS P
    "0P00000MY5.IR",    # KLP OBLIGASJON 1 ÅR P
    "0P00000O8G.IR",    # KLP OBLIGASJON 3 ÅR P
    "0P00017YPU.IR",    # KLP AksjeAsia Indeks P
    "0P0000TJ5D.IR",    # KLP AksjeFremvoksende Markeder Indeks P
    "0P0001BWAR.IR"     # KLP AksjeGlobal Small Cap Indeks P
}

@functions_framework.http
def asset_prices_daily(request):
    try:
        request_json = request.get_json(silent=True)
        target_date = request_json.get("date") if request_json else None
        if target_date:
            date_obj = datetime.strptime(target_date, "%Y-%m-%d").date()
        else:
            date_obj = (datetime.today() - timedelta(days=1)).date()
        logger.info(f"Processing asset prices for date: {date_obj}")

        # Asset ids which are active in portfolio
        active_asset_ids_query = f"""
            select distinct asset_id
            from (
                SELECT date, asset_id, sum(purchase_amounts) as total_purchase_amounts
                FROM `{PROJECT_ID}.marts_facts.fct_purchase_amounts`
                group by date, asset_id)
            WHERE date = DATE('{date_obj.isoformat()}')
                AND (total_purchase_amounts > 0 OR total_purchase_amounts != 999999)
        """

        # Read Google Sheets
        try:
            creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
            gs_client = gspread.Client(auth=creds)
            gs_client.session = AuthorizedSession(creds)
            sheet = gs_client.open_by_key(GOOGLE_SHEETS_ID).worksheet(SHEET_NAME)
            assets = sheet.get_all_records()
        except Exception:
            logger.error("Failed to load assets from Google Sheets:\n%s", traceback.format_exc())
            return "Failed to read assets sheet", 500

        # Fetch active asset_ids
        bq = bigquery.Client()
        if request_json and request_json.get("asset_ids"):
            active_asset_ids = set(request_json.get("asset_ids"))
            logger.info(f"Requested asset_id filter: {active_asset_ids}")
        else:
            try:
                active_asset_ids = {
                    row["asset_id"]
                    for row in bq.query(active_asset_ids_query).result()
                    if row["asset_id"]
                }
                logger.info(f"Active asset_id count on {date_obj}: {len(active_asset_ids)}")
            except Exception:
                logger.error("Failed to fetch excluded asset_ids:\n%s", traceback.format_exc())
                return "Failed to fetch exclusion list", 500

        # Select active assets
        assets = [row for row in assets if row.get("asset_id") in active_asset_ids]
        logger.info(f"Active asset count: {len(assets)}")

        # Preparing temp table for data loading
        try:
            bq.delete_table(ASSET_PRICES_TEMP_TABLE, not_found_ok=True)
            table = bigquery.Table(ASSET_PRICES_TEMP_TABLE, schema=ASSET_PRICES_SCHEMA)
            bq.create_table(table)
            logger.info(f"Created temp table {ASSET_PRICES_TEMP_TABLE}")
        except Exception as e:
            logger.error(f"Failed to create temp table: {e}")
            return f"Error creating temp table: {e}", 500

        # Fetch prices
        total_requested = 0
        total_inserted = 0
        MAX_FALLBACK_DAYS = 3   # Yesterday´s KLP price data started not to be available today morning since late 2025. So we get D-2 data if data price is not available.

        for i in range(0, len(assets), BATCH_SIZE):
            batch = assets[i:i + BATCH_SIZE]
            
            rows_to_insert = []
            for asset in batch:
                asset_name = asset.get("asset_name")
                asset_id = asset.get("asset_id")
                currency = asset.get("currency")

                if not asset_id:
                    continue

                price = None
                total_requested += 1

                # Delayed assets get multiple fallback attempts; others get 1 attempt only
                max_attempts = MAX_FALLBACK_DAYS if asset_id in DELAYED_ASSET_IDS else 1

                for fallback in range(max_attempts):
                    try_date = date_obj - timedelta(days=fallback)
                    try:
                        ticker = yf.Ticker(asset_id)
                        hist = ticker.history(
                            start=str(try_date),
                            end=str(try_date + timedelta(days=1))
                        )
                        if not hist.empty:
                            close_price = hist["Close"].iloc[0]
                            if not pd.isna(close_price):
                                price = round(float(close_price), 2)
                                rows_to_insert.append({
                                    "date": date_obj.isoformat(),   # Always use the target date, not date from API call
                                    "asset_name": asset_name,
                                    "asset_id": asset_id,
                                    "price": price,
                                    "currency": currency
                                })
                                total_inserted += 1
                                if fallback > 0:
                                    logger.info(f"Fallback D-{fallback}: used {try_date} price for {asset_id}")
                                break  # Successfully got price, no need for further fallback
                    except Exception as e:
                        logger.warning(f"Error fetching price for {asset_id} on {date_obj}: {e}")

                if price is None:
                    logger.warning(f"No price found for {asset_id} (tried {max_attempts} day(s))")

            if rows_to_insert:
                # Load data into temp table
                try:
                    job_config = bigquery.LoadJobConfig(schema=ASSET_PRICES_SCHEMA, 
                                                        write_disposition="WRITE_APPEND")
                    load_job = bq.load_table_from_json(rows_to_insert, ASSET_PRICES_TEMP_TABLE, job_config=job_config)
                    load_job.result()
                    logger.debug(f"Loaded temp table with asset prices data for {date_obj} for batch {i + BATCH_SIZE}")
                except Exception:
                    logger.error("Failed to load data into temp table:\n%s", traceback.format_exc())
                    return "Failed to load data into temp table", 500
            
            # Reset intermediate data. Otherwise memory overflow occurs.
            del rows_to_insert
            del batch
            gc.collect()

        # MERGE temp table into target
        merge_sql = f"""
            MERGE `{ASSET_PRICES_TABLE}` T
            USING `{ASSET_PRICES_TEMP_TABLE}` S
            ON T.date = S.date AND T.asset_id = S.asset_id
            WHEN MATCHED THEN
                UPDATE SET T.asset_name = S.asset_name, 
                    T.price = S.price,
                    T.currency = S.currency
            WHEN NOT MATCHED THEN
                INSERT (date, asset_name, asset_id, price, currency)
                VALUES(S.date, S.asset_name, S.asset_id, S.price, S.currency)
        """
        bq.query(merge_sql).result()
        logger.info(f"Upserted asset prices data for {date_obj}")

        # Clean up temp table
        bq.delete_table(ASSET_PRICES_TEMP_TABLE, not_found_ok=True)
        logger.info(f"Deleted temp table {ASSET_PRICES_TEMP_TABLE}")

        if total_inserted == 0:
            return "No price data inserted.", 200
        logger.info(f"Total requested: {total_requested}, Total inserted: {total_inserted}")
        return f"Inserted {total_inserted} rows.", 200

    except Exception as e:
        logger.error("Exception occurred:\n%s", traceback.format_exc())
        return f"Internal Server Error: {e}", 500
