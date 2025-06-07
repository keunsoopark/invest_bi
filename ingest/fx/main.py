import functions_framework
import requests
import xml.etree.ElementTree as ET
from google.cloud import bigquery
import logging

URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
PROJECT_ID = "xnwk-462111"
DATASET_NAME = "src_external"
TABLE_NAME = "fx"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

@functions_framework.http
def fx_daily(request):
    response = requests.get(URL)
    root = ET.fromstring(response.content)

    for cube_time in root.findall(".//{http://www.ecb.int/vocabulary/2002-08-01/eurofxref}Cube/{http://www.ecb.int/vocabulary/2002-08-01/eurofxref}Cube"):
        date = cube_time.attrib["time"]
        rates = {
            c.attrib['currency']: float(c.attrib['rate']) 
            for c in cube_time.findall('{http://www.ecb.int/vocabulary/2002-08-01/eurofxref}Cube')
        }

        if 'USD' in rates and 'NOK' in rates and 'KRW' in rates:
            usd_nok = (1 / rates['USD']) * rates['NOK']
            nok_krw = (1 / rates['NOK']) * rates['KRW']

            bq = bigquery.Client()
            table_ref = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"
            temp_table_ref = f"{table_ref}_temp"

            row = [{
                "date": date,
                "USDNOK": usd_nok,
                "NOKKRW": nok_krw
            }]

            schema = [
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("USDNOK", "FLOAT"),
                bigquery.SchemaField("NOKKRW", "FLOAT")
            ]

            try:
                bq.delete_table(temp_table_ref, not_found_ok=True)
                table = bigquery.Table(temp_table_ref, schema=schema)
                bq.create_table(table)
                logger.info(f"Created temp table {temp_table_ref}")
            except Exception as e:
                logger.error(f"Failed to create temp table: {e}")
                return f"Error creating temp table: {e}", 500

            # Load data into temp table
            job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
            load_job = bq.load_table_from_json(row, temp_table_ref, job_config=job_config)
            load_job.result()
            logger.info(f"Loaded temp table with FX data for {date}")

            # MERGE temp table into target
            merge_sql = f"""
                MERGE `{table_ref}` T
                USING `{temp_table_ref}` S
                ON T.date = S.date
                WHEN MATCHED THEN
                  UPDATE SET T.USDNOK = S.USDNOK, T.NOKKRW = S.NOKKRW
                WHEN NOT MATCHED THEN
                  INSERT (date, USDNOK, NOKKRW) VALUES(S.date, S.USDNOK, S.NOKKRW)
            """
            bq.query(merge_sql).result()
            logger.info(f"Upserted FX data for {date}")

            # Clean up temp table
            bq.delete_table(temp_table_ref, not_found_ok=True)
            logger.info(f"Deleted temp table {temp_table_ref}")

            return f"Upserted FX data for {date}", 200

    logger.warning("No valid FX data found in ECB feed")
    return "No valid FX data found", 404
