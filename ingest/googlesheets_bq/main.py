import functions_framework
from google.cloud import bigquery
import gspread
import google.auth
from google.auth.transport.requests import AuthorizedSession
from datetime import datetime
import logging
import traceback

GOOGLE_SHEETS_ID = "17ZHn4wq0Ga36_qEY6Qdt8jxzCnrSe48GRK-OvqUO1HQ"
PROJECT_ID = "xnwk-462111"
DATASET_NAME = "src_googlesheets"
TABLE_NAME = "transactions"
TEMP_TABLE_NAME = "transactions_staging"

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@functions_framework.http
def ingest_transactions(request):
    try:
        bq = bigquery.Client()
        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
        gc = gspread.Client(auth=creds)
        gc.session = AuthorizedSession(creds)
        sheet = gc.open_by_key(GOOGLE_SHEETS_ID).worksheet(TABLE_NAME)

        rows = sheet.get_all_records()
        table_id = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"
        temp_table_id = f"{PROJECT_ID}.{DATASET_NAME}.{TEMP_TABLE_NAME}"

        # Fetch existing versioned keys
        existing_rows = {
            f"{row['hash_key']}": row['version']
            for row in bq.query(f"""
                SELECT
                    CONCAT(
                        CAST(date AS STRING), 
                        asset_name, 
                        FORMAT('%.2f', price), 
                        CAST(amounts AS STRING)
                    ) AS hash_key,
                    version
                FROM `{table_id}`
            """).result()
        }

        new_or_updated_rows = []
        for r in rows:
            parsed_date = datetime.strptime(r["date"], "%Y/%m/%d").date().isoformat()
            key = f"{parsed_date}{r['asset_name']}{float(r['price']):.2f}{r['amounts']}"
            version = r.get("version")
            existing_version = existing_rows.get(key)

            if existing_version is None or str(version) != str(existing_version):
                new_or_updated_rows.append({
                    "date": parsed_date,
                    "asset_name": r["asset_name"],
                    "asset_id": r["asset_id"],
                    "price": float(r["price"]),
                    "currency": r["currency"],
                    "amounts": int(r["amounts"]),
                    "strategy_name": r["strategy_name"],
                    "strategy_details": r.get("strategy_details"),
                    "version": version,
                })

        if not new_or_updated_rows:
            logger.info("No new or updated rows to ingest")
            return "No updates", 200

        # Load into temp table
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("asset_name", "STRING"),
                bigquery.SchemaField("asset_id", "STRING"),
                bigquery.SchemaField("price", "FLOAT"),
                bigquery.SchemaField("currency", "STRING"),
                bigquery.SchemaField("amounts", "INT64"),
                bigquery.SchemaField("strategy_name", "STRING"),
                bigquery.SchemaField("strategy_details", "STRING"),
                bigquery.SchemaField("version", "STRING"),
            ],
            write_disposition="WRITE_TRUNCATE"
        )

        load_job = bq.load_table_from_json(new_or_updated_rows, temp_table_id, job_config=job_config)
        load_job.result()
        logger.info(f"Loaded {len(new_or_updated_rows)} rows into staging table")

        # MERGE into final table
        merge_sql = f"""
            MERGE `{table_id}` T
            USING `{temp_table_id}` S
            ON
              T.date = S.date AND
              T.asset_name = S.asset_name AND
              T.price = S.price AND
              T.amounts = S.amounts
            WHEN MATCHED AND T.version != S.version THEN
              UPDATE SET
                date = S.date,
                asset_name = S.asset_name,
                asset_id = S.asset_id,
                price = S.price,
                currency = S.currency,
                amounts = S.amounts,
                strategy_name = S.strategy_name,
                strategy_details = S.strategy_details,
                version = S.version
            WHEN NOT MATCHED THEN
              INSERT (
                date, asset_name, asset_id, price, currency,
                amounts, strategy_name, strategy_details, version
              )
              VALUES (
                S.date, S.asset_name, S.asset_id, S.price, S.currency,
                S.amounts, S.strategy_name, S.strategy_details, S.version
              )
        """
        bq.query(merge_sql).result()
        logger.info("Merge completed")
        return f"Upserted {len(new_or_updated_rows)} rows", 200

    except Exception as e:
        logger.error("Exception occurred:\n%s", traceback.format_exc())
        return f"Internal Server Error: {e}", 500
