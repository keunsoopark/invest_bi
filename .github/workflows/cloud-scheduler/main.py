import requests
import jwt
import time
import os
from google.cloud import secretmanager
import functions_framework

# App details from GitHub App settings
APP_ID = "1743825"
INSTALLATION_ID = "79710160"
REPO = "keunsoopark/invest_bi"
REF = "main"

@functions_framework.http
def trigger_github_action(request):
    try:
        # These must be set as env vars
        workflow_id = os.environ["WORKFLOW_ID"]
        # Read private key from the file that was mounted by Secret Manager
        with open("/secrets/github-app-private-key", "r") as f:
            PRIVATE_KEY = f.read()

        # Step 1: JWT for GitHub App
        payload = {
            "iat": int(time.time()) - 60,
            "exp": int(time.time()) + (10 * 60),
            "iss": APP_ID
        }
        jwt_token = jwt.encode(payload, PRIVATE_KEY, algorithm="RS256")

        # Step 2: Get Installation Token
        headers = {"Authorization": f"Bearer {jwt_token}", "Accept": "application/vnd.github+json"}
        resp = requests.post(
            f"https://api.github.com/app/installations/{INSTALLATION_ID}/access_tokens",
            headers=headers
        )
        resp.raise_for_status()
        installation_token = resp.json()["token"]

        # Step 3: Dispatch workflow
        headers = {
            "Authorization": f"token {installation_token}",
            "Accept": "application/vnd.github+json"
        }
        data = {"ref": REF}
        workflow_dispatch_url = f"https://api.github.com/repos/{REPO}/actions/workflows/{workflow_id}/dispatches"
        r = requests.post(workflow_dispatch_url, json=data, headers=headers)
        r.raise_for_status()
        return f"Triggered workflow: {r.status_code}, {r.text}", 200
    except Exception as e:
        return f"Error: {e}", 500
