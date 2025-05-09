import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import csv
import os

MANAGERS_CSV = "data/managers.csv"
FILINGS_CSV = "data/filings.csv"
ERRORS_CSV = "data/error_managers.csv"
BASE_URL = "https://13f.info"

def scrape_filings_for_manager(manager, error_log):
    filings = []
    fund_name = manager.get("fund_name")
    manager_url = manager.get("manager_url")

    try:
        response = requests.get(manager_url)
        if response.status_code != 200:
            raise Exception(f"HTTP {response.status_code}")

        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table")

        if not table:
            raise Exception("No filings table found")

        headers = [th.text.strip().lower() for th in table.find("thead").find_all("th")]
        header_map = {header: idx for idx, header in enumerate(headers)}
        required_headers = ["quarter", "holdings", "value ($000)", "top holdings", "form type", "date filed"]

        if not all(key in header_map for key in required_headers):
            raise Exception("Missing one or more required headers")

        for row in table.find("tbody").find_all("tr"):
            cols = row.find_all("td")
            if len(cols) < len(required_headers):
                continue

            try:
                form_type = cols[header_map["form type"]].text.strip()
                if form_type != "13F-HR":
                    continue

                quarter = cols[header_map["quarter"]].text.strip()
                filing_date = cols[header_map["date filed"]].text.strip()
                filing_link = cols[header_map["quarter"]].find("a")
                filing_href = filing_link["href"].strip() if filing_link else None
                filing_url = BASE_URL + filing_href if filing_href else None

                if filing_url:
                    filings.append({
                        "fund_name": fund_name,
                        "quarter": quarter,
                        "filing_date": filing_date,
                        "form_type": form_type,
                        "filing_url": filing_url
                    })
            except Exception as e:
                print(f"[!] Row parsing error: {e}")
                continue

    except Exception as e:
        print(f"[ERROR] {fund_name}: {e}")
        error_log.append({"fund_name": fund_name, "manager_url": manager_url, "error": str(e)})

    return filings

def scrape_all_13fhr_filings():
    print("Loading managers from CSV...")
    df = pd.read_csv(MANAGERS_CSV)
    all_filings = []
    error_log = []

    for i, row in df.iterrows():
        manager = row.to_dict()
        print(f"Scraping filings for: {manager.get('fund_name')} ({i+1}/{len(df)})")
        filings = scrape_filings_for_manager(manager, error_log)
        all_filings.extend(filings)

        if (i + 1) % 10 == 0:
            if all_filings:
                temp_df = pd.DataFrame(all_filings)
                temp_df.to_csv(FILINGS_CSV, mode='a', header=not os.path.exists(FILINGS_CSV),
                               index=False, quotechar='"', quoting=csv.QUOTE_ALL)
                print(f"[✓] Saved chunk of {len(temp_df)} filings (up to manager {i+1})")
                all_filings.clear()

        time.sleep(0.5)

    if all_filings:
        temp_df = pd.DataFrame(all_filings)
        temp_df.to_csv(FILINGS_CSV, mode='a', header=not os.path.exists(FILINGS_CSV),
                       index=False, quotechar='"', quoting=csv.QUOTE_ALL)
        print(f"[✓] Final write: {len(temp_df)} filings")

    if error_log:
        err_df = pd.DataFrame(error_log)
        err_df.to_csv(ERRORS_CSV, index=False, quotechar='"', quoting=csv.QUOTE_ALL)
        print(f"[!] Logged {len(err_df)} errors to {ERRORS_CSV}")

    print("[✓] Scraping complete.")

if __name__ == "__main__":
    scrape_all_13fhr_filings()