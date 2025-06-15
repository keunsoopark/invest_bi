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
INACTIVE_ASSET_IDS_QUERY = f"""
    SELECT DISTINCT asset_id
    FROM `{PROJECT_ID}.marts_facts.fct_status`
    WHERE purchase_amounts < 1 OR purchase_amounts = 999999
"""

@functions_framework.http
def asset_prices_daily(request):
    try:
        request_json = request.get_json(silent=True)
        target_date = request_json.get("date") if request_json else None
        if target_date:
            date_obj = datetime.strptime(target_date, "%Y-%m-%d").date()
        else:
            date_obj = (datetime.today() - timedelta(days=1)).date()

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

        # Fetch inactive asset_ids
        bq = bigquery.Client()
        try:
            excluded_asset_ids = {
                row["asset_id"]
                for row in bq.query(INACTIVE_ASSET_IDS_QUERY).result()
                if row["asset_id"]
            }
            logger.info(f"Excluded asset_id count: {len(excluded_asset_ids)}")
        except Exception:
            logger.error("Failed to fetch excluded asset_ids:\n%s", traceback.format_exc())
            return "Failed to fetch exclusion list", 500

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
        for i in range(0, len(assets), BATCH_SIZE):
            batch = assets[i:i + BATCH_SIZE]
            
            rows_to_insert = []
            for asset in batch:
                asset_name = asset.get("asset_name")
                asset_id = asset.get("asset_id")
                currency = asset.get("currency")

                if not asset_id or asset_id in excluded_asset_ids:
                    continue

                price = None
                try:
                    ticker = yf.Ticker(asset_id)
                    hist = ticker.history(
                        start=str(date_obj),
                        end=str(date_obj + timedelta(days=1))
                    )
                    total_requested += 1
                    if not hist.empty:
                        data_date = hist.index[0].date()
                        close_price = hist["Close"].iloc[0]
                        if not pd.isna(close_price):
                            price = round(float(close_price), 2)
                            rows_to_insert.append({
                                "date": data_date.isoformat(),
                                "asset_name": asset_name,
                                "asset_id": asset_id,
                                "price": price,
                                "currency": currency
                            })
                            total_inserted += 1
                except Exception as e:
                    logger.warning(f"Error fetching price for {asset_id} on {date_obj}: {e}")

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
