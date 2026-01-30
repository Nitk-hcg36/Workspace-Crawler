import os
import json
import pandas as pd
import requests
import smartsheet
import urllib3

urllib3.disable_warnings()

# Smartsheet credentials
SMARTSHEET_TOKEN = os.getenv("SS_TOKEN")  # Make sure this is in your GitHub secrets
SHEET_ID = int(os.getenv("SM_SHEET_ID"))  # Sheet ID for User Management

# Columns in your sheet
SHEET_COLUMNS = [
    "id",
    "name",
    "accessLevel",
    "permalink",
    "createdAt",
    "modifiedAt"
]

# API to fetch Smartsheet users
API_TOKEN = SMARTSHEET_TOKEN
URL = "https://api.smartsheet.com/2.0/sights"
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Accept": "application/json"
}

# --- Helper functions ---

def fetch_all_users(page_size=300):
    """Fetch all webhook from Smartsheet with pagination."""
    all_webhooks = []
    page = 1
    while True:
        params = {"page": page, "pageSize": page_size}
        resp = requests.get(URL, headers=HEADERS, params=params, verify=False)
        resp.raise_for_status()
        data = resp.json()
        users  = data.get("data", [])
        all_webhooks.extend(users)
        print(f"✅ Fetched page {page} | Users: {len(users)}")
        if len(users) < page_size:
            break
        page += 1
    print(f"\n🎯 Total webhooks fetched: {len(all_webhooks)}")
    return all_webhooks


def build_smartsheet_row(user, col_map):
    """Convert user dict to Smartsheet Row object."""
    cells = []
    for col_title in SHEET_COLUMNS:
        val = None
        # Map column values
        if col_title == "id":
            val = user.get("id", "")
        elif col_title == "name":
            val = user.get("name", "")
        elif col_title == "accessLevel":
            val = user.get("accessLevel", "")
        elif col_title == "permalink":
            val = user.get("permalink", "")
        elif col_title == "createdAt":
            val = user.get("createdAt", "")
        elif col_title == "modifiedAt":
            val = user.get("modifiedAt", "")
        else:  # Checkbox columns
            val = bool(user.get(col_title.replace(" ", "").lower(), False))
        # Avoid None
        if val is None:
            val = ""
        # Create Cell
        cell = smartsheet.models.Cell({
            "column_id": col_map[col_title],
            "value": val
        })
        cells.append(cell)

    row = smartsheet.models.Row()
    row.to_top = True
    row.cells = cells
    return row


def delete_all_rows(ss_client, sheet_id, batch_size=200):
    """Delete all existing rows in the sheet in batches."""
    sheet = ss_client.Sheets.get_sheet(sheet_id)
    all_row_ids = [row.id for row in sheet.rows]
    print(f"⚠️ Deleting {len(all_row_ids)} existing rows...")

    for i in range(0, len(all_row_ids), batch_size):
        batch = all_row_ids[i:i+batch_size]
        if batch:
            ss_client.Sheets.delete_rows(sheet_id, batch)
            print(f"Deleted rows {i+1} to {i+len(batch)}")


def push_users_to_smartsheet(users):
    """Push new user data to Smartsheet."""
    ss_client = smartsheet.Smartsheet(SMARTSHEET_TOKEN)
    ss_client.errors_as_exceptions(True)

    # Get column mapping
    sheet = ss_client.Sheets.get_sheet(SHEET_ID)
    col_map = {col.title: col.id for col in sheet.columns}

    # Delete existing rows first
    delete_all_rows(ss_client, SHEET_ID)

    # Build row objects
    rows = [build_smartsheet_row(u, col_map) for u in users]

    # Add rows in batches (max 500 per API call)
    batch_size = 200
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        ss_client.Sheets.add_rows(SHEET_ID, batch)
        print(f"✅ Added rows {i+1} to {i+len(batch)}")

# --- Main Execution ---
def main():
    users = fetch_all_users()
    push_users_to_smartsheet(users)


if __name__ == "__main__":
    main()
