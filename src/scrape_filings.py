import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import csv

MANAGERS_CSV = "data/managers.csv"
BASE_URL = "https://13f.info"

def scrape_filings_for_manager(manager):
    """
    Given a manager dictionary (from managers.csv), scrape all 13F-HR filings
    from their profile page.

    Returns:
        List[Dict]: Each dict contains metadata for a 13F-HR filing.
    """
    filings = []
    fund_name = manager.get("fund_name")
    manager_url = manager.get("manager_url")

    try:
        response = requests.get(manager_url)
        if response.status_code != 200:
            print(f"[ERROR] Failed to load manager page: {manager_url}")
            return filings

        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table")

        if not table:
            print(f"[WARNING] No filings table found for: {fund_name}")
            return filings

        # Map table headers to indices
        headers = [th.text.strip().lower() for th in table.find("thead").find_all("th")]
        header_map = {header: idx for idx, header in enumerate(headers)}

        required_headers = ["quarter", "holdings", "value ($000)", "top holdings", "form type", "date filed"]
        if not all(key in header_map for key in required_headers):
            print(f"[ERROR] Missing required headers for {fund_name}")
            return filings

        for row in table.find("tbody").find_all("tr"):
            cols = row.find_all("td")
            if len(cols) < len(required_headers):
                continue  # Skip incomplete rows

            try:
                form_type = cols[header_map["form type"]].text.strip()
                if form_type != "13F-HR":
                    continue

                quarter = cols[header_map["quarter"]].text.strip()
                filing_date = cols[header_map["date filed"]].text.strip()

                # Filing URL comes from a hyperlink inside the first cell (quarter)
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
                print(f"[!] Skipped row due to parsing error: {e}")
                continue

    except Exception as e:
        print(f"[EXCEPTION] Failed to scrape filings for {fund_name}: {e}")

    return filings

def scrape_all_13fhr_filings():
    """
    Loads all managers and scrapes their 13F-HR filings.

    Returns:
        DataFrame: All valid 13F-HR filing metadata across all managers.
    """
    print("Loading managers from CSV...")
    df = pd.read_csv(MANAGERS_CSV)
    all_filings = []

    for i, row in df.iterrows():
        manager = row.to_dict()
        print(f"🔍 Scraping filings for: {manager.get('fund_name')} ({i+1}/{len(df)})")
        filings = scrape_filings_for_manager(manager)
        all_filings.extend(filings)
        time.sleep(0.5)  

    return pd.DataFrame(all_filings)

if __name__ == "__main__":
    filings_df = scrape_all_13fhr_filings()

    output_path = "data/filings.csv"
    filings_df.to_csv(output_path, index=False, quotechar='"', quoting=csv.QUOTE_ALL)
    print(f"Saved {len(filings_df)} filings to {output_path}")
