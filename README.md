# Invest BI

If you have any auth problem, run `gcloud auth application-default login`.

In running dbt command, you would still have auth error message because some source tables are external tables based on Google Drive. Run this instead: `gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/drive`.

## Data modeling

EDM of source data: https://dbdiagram.io/d/investment-6842b873ba2a4ac57b224ea2


## Ingest

### Google Sheets to BigQuery

To deploy it to Google Cloud Run Function:

```
gcloud functions deploy ingest_transactions \
  --gen2 \
  --runtime python311 \
  --region europe-north1 \
  --entry-point ingest_transactions \
  --source . \
  --trigger-http \
  --allow-unauthenticated \
  --service-account ingest-transactions@xnwk-462111.iam.gserviceaccount.com
```

Deployed function: https://europe-north1-xnwk-462111.cloudfunctions.net/ingest_transactions


### fx daily
```
gcloud functions deploy fx_daily \
  --gen2 \
  --runtime python311 \
  --region europe-north1 \
  --entry-point fx_daily \
  --source . \
  --trigger-http \
  --allow-unauthenticated \
  --service-account ingest-transactions@xnwk-462111.iam.gserviceaccount.com
```

Deployed function: https://europe-north1-xnwk-462111.cloudfunctions.net/fx_daily


### Asset prices daily

> [!NOTE]  
> Cloud Run cannot read data in Google Sheets via BigQuery external table, although its service account has all IAM permissions for Google Drive and BigQuery, because Google Drive access requires OAuth scope - like user consent when we use user account to access to Google Drive - https://cloud.google.com/bigquery/docs/external-data-drive?utm_source=chatgpt.com#enable-google-drive
> That is why this solution needs to access to Google sheets directly to read `assets` sheet.

To deploy it to Google Cloud Run Function:

```
gcloud functions deploy asset_prices_daily \
  --gen2 \
  --runtime python311 \
  --region europe-north1 \
  --entry-point asset_prices_daily \
  --source . \
  --trigger-http \
  --allow-unauthenticated \
  --service-account ingest-transactions@xnwk-462111.iam.gserviceaccount.com \
  --timeout=180
```

Deployed function: https://europe-north1-xnwk-462111.cloudfunctions.net/asset_prices_daily


## dbt

### Useful commands

Create model yaml file
```
dbt run-operation generate_model_yaml --args '{"model_names": ["stg_fx"]}'    
```

