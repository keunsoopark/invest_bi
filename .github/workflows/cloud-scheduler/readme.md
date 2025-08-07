## How to deploy

dbt daily run
```
gcloud functions deploy dbt-run-daily-gha \
  --gen2 \
  --runtime python311 \
  --region europe-north1 \
  --source . \
  --entry-point trigger_github_action \
  --trigger-http \
  --set-env-vars GITHUB_APP_PRIVATE_KEY_SECRET_ID=projects/475695677196/secrets/dbt-gha-github-app-key/versions/latest,WORKFLOW_ID=dbt-daily-run.yml \
  --allow-unauthenticated
```

dbt daily run fct_purchase_amounts
```
gcloud functions deploy dbt-run-daily-gha \
  --gen2 \
  --runtime python311 \
  --region europe-north1 \
  --source . \
  --entry-point trigger_github_action \
  --trigger-http \
  --set-env-vars GITHUB_APP_PRIVATE_KEY_SECRET_ID=projects/475695677196/secrets/dbt-gha-github-app-key/versions/latest,WORKFLOW_ID=dbt-update-purchase-amounts.yml \
  --allow-unauthenticated
```

Github app is used for authentificating Cloud run function to Github.  
Github app private key is stored in Secret Manager.
