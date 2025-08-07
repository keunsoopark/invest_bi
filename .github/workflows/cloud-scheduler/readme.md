## How to deploy

dbt daily run
```
gcloud functions deploy dbt-run-gha \
  --gen2 \
  --runtime python311 \
  --region europe-north1 \
  --source . \
  --entry-point trigger_github_action \
  --trigger-http \
  --set-env-vars=WORKFLOW_ID=dbt-daily-run.yaml \
  --set-secrets="github-app-private-key=dbt-gha-github-app-key:latest" \
  --allow-unauthenticated
```

dbt daily run fct_purchase_amounts
```
gcloud functions deploy dbt-update-fct-purchase-amounts-gha \
  --gen2 \
  --runtime python311 \
  --region europe-north1 \
  --source . \
  --entry-point trigger_github_action \
  --trigger-http \
  --set-env-vars=WORKFLOW_ID=dbt-update-purchase-amounts.yaml \
  --set-secrets="github-app-private-key=dbt-gha-github-app-key:latest" \
  --allow-unauthenticated
```

Github app is used for authentificating Cloud run function to Github.  
Github app private key is stored in Secret Manager.
