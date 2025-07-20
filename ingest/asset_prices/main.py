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

        # Asset ids which are active in balance based on fct_status table
        active_asset_ids_query = f"""
            select distinct asset_id
            from (
                SELECT date, asset_id, sum(purchase_amounts) as total_purchase_amounts
                FROM `{PROJECT_ID}.marts_facts.fct_status`
                group by date, asset_id)
            WHERE date = DATE('{date_obj.isoformat()}')
                AND (total_purchase_amounts > 0 OR total_purchase_amounts != 999999)
        """
        # If there are new rows in transaction, which are not listed in fct_status table yet
        # Since omitting asset_id in transactions is allowed, we need to get asset_id by joining with assets table.
        # After adding new rows in transaction, run "dbt run -s +fct_purchase --full-refresh" first. And then run asset_price reader
        newly_traded_asset_names_query = f"""
            select distinct asset_name
            from `{PROJECT_ID}.marts_facts.fct_purchase`
            where date = DATE('{date_obj.isoformat()}')
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
        if request_json.get("asset_ids"):
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

                newly_traded_asset_names = {
                    row["asset_name"]
                    for row in bq.query(newly_traded_asset_names_query).result()
                    if row["asset_name"]
                }
                newly_traded_asset_ids = {
                    row["asset_id"]
                    for row in assets
                    if row.get("asset_name") in newly_traded_asset_names and row.get("asset_id")
                }

                logger.info(f"newly_traded_asset_names: {newly_traded_asset_names}")
                logger.info(f"newly_traded_asset_ids: {newly_traded_asset_ids}")

                logger.info(f"Newly traded asset_id count on {date_obj}: {len(newly_traded_asset_ids)}")

                active_asset_ids = active_asset_ids.union(newly_traded_asset_ids)
            except Exception:
                logger.error("Failed to fetch excluded asset_ids:\n%s", traceback.format_exc())
                return "Failed to fetch exclusion list", 500

        # Select active assets
        assets = [row for row in assets if row.get("asset_id") in active_asset_ids]
        logger.info(f"Filtered asset count: {len(assets)}")

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

                if not asset_id:
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
