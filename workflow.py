import warnings
# Silence only that gspread deprecation about arg order
warnings.filterwarnings(
    "ignore",
    message=".*Method signature's arguments 'range_name' and 'values' will change their order.*",
    category=DeprecationWarning,
)

import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from nltk.sentiment import SentimentIntensityAnalyzer
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

def load_config(path="config.json"):
    with open(path) as f:
        return json.load(f)

def get_metabase_token(cfg):
    url = cfg["metabase"]["url"].rstrip("/")
    resp = requests.post(
        f"{url}/api/session",
        json={
            "username": cfg["metabase"]["username"],
            "password": cfg["metabase"]["password"],
        },
    )
    resp.raise_for_status()
    return resp.json()["id"]

def fetch_metabase_df(cfg, token, start_date, end_date):
    url = cfg["metabase"]["url"].rstrip("/")
    card_id = cfg["metabase_question_id"]
    headers = {"X-Metabase-Session": token}
    params = [
        {
            "type": "date",
            "target": ["variable", ["template-tag", "start_date"]],
            "value": start_date,
        },
        {
            "type": "date",
            "target": ["variable", ["template-tag", "end_date"]],
            "value": end_date,
        },
    ]
    resp = requests.post(
        f"{url}/api/card/{card_id}/query/json",
        headers=headers,
        json={"parameters": params},
    )
    resp.raise_for_status()
    return pd.DataFrame(resp.json())

def classify_responses(df):
    sia = SentimentIntensityAnalyzer()
    NOT_INTERESTED = ['stop', 'no', 'not interested', 'unsubscribe']
    VERY_INTERESTED = ['asap', 'urgent', 'right away', 'please', 'definitely', 'absolutely']

    def classify(text):
        if not isinstance(text, str) or not text.strip():
            return 'Not Interested'
        t = text.strip().lower()
        if any(kw in t for kw in NOT_INTERESTED):
            return 'Not Interested'
        if any(kw in t for kw in VERY_INTERESTED):
            return 'Very Interested'
        score = sia.polarity_scores(t)['compound']
        if score >= 0.6:
            return 'Very Interested'
        if score <= -0.05:
            return 'Not Interested'
        return 'Interested'

    df['classification'] = df['last_clinician_response'].apply(classify)
    return df

def clean_illegal(df):
    pattern = ILLEGAL_CHARACTERS_RE
    return df.apply(lambda col: col.map(
        lambda v: pattern.sub("", v) if isinstance(v, str) else v
    ))

def update_sheet(cfg, df, pivots):
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        cfg["google_sheets"]["credentials_path"], scope
    )
    client = gspread.authorize(creds)
    ss = client.open_by_key(cfg["google_sheets"]["spreadsheet_id"])

    # Write main sheet
    main_ws = ss.sheet1
    main_ws.clear()
    all_values = [df.columns.tolist()] + df.values.tolist()
    main_ws.update(values=all_values, range_name="A1")

    # Write each pivot
    for title, pivot in pivots.items():
        try:
            ws = ss.worksheet(title)
        except gspread.WorksheetNotFound:
            ws = ss.add_worksheet(
                title=title,
                rows=str(len(pivot) + 1),
                cols=str(len(pivot.columns))
            )
        ws.clear()
        pivot_df = pivot.reset_index()
        values = [pivot_df.columns.tolist()] + pivot_df.values.tolist()
        ws.update(values=values, range_name="A1")

def get_date_range():
    tz = ZoneInfo("America/Mexico_City")
    today = datetime.now(tz).date()
    start = today - timedelta(days=7)
    return start.isoformat(), today.isoformat()

def main(start_date, end_date):
    cfg = load_config()
    token = get_metabase_token(cfg)
    df = fetch_metabase_df(cfg, token, start_date, end_date)
    df = classify_responses(df)
    df = clean_illegal(df)

    pivots = {
        "By Application Status": pd.crosstab(df['application_status'], df['classification']),
        "By Lead Type":           pd.crosstab(df['lead_type'], df['classification']),
        "By UTM Source":          pd.crosstab(df['utm_source'], df['classification']),
        "By Customer":            pd.crosstab(df['customer_name'], df['classification']),
        "By Cust & Lead Type":    pd.crosstab(
                                      [df['customer_name'], df['lead_type']],
                                      df['classification']
                                  ),
    }

    update_sheet(cfg, df, pivots)
    print(f"✅ Workflow run complete: {len(df)} rows processed from {start_date} to {end_date}")

if __name__ == "__main__":
    # If you pass dates manually, it’ll use those.
    # Otherwise it auto‐computes the last 7 days.
    if len(sys.argv) == 3:
        sd, ed = sys.argv[1], sys.argv[2]
    else:
        sd, ed = get_date_range()
    main(sd, ed)
