# src/scrape_managers.py

import requests
from bs4 import BeautifulSoup
import pandas as pd

# Base URLs for scraping
BASE_URL = "https://13f.info"
MANAGERS_URL = f"{BASE_URL}/managers"

def scrape_all_managers():
    """
    Scrape the list of all fund managers from https://13f.info/managers.

    Returns:
        List[Dict]: Each dict contains manager metadata including:
                    - fund_name
                    - location
                    - most_recent_filing
                    - num_holdings
                    - holdings_value
                    - manager_url (link to manager profile)
    """
    print("Fetching all fund managers...")

    response = requests.get(MANAGERS_URL)
    if response.status_code != 200:
        raise Exception(f"Failed to load page: {MANAGERS_URL} (status {response.status_code})")

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")

    if not table:
        raise Exception("Could not find managers table on the page.")

    # Extract headers and map header names to column indexes
    headers = table.find("thead").find_all("th")
    header_map = {th.get_text(strip=True).lower(): idx for idx, th in enumerate(headers)}

    required_headers = ["name", "location", "most recent filing", "num holdings", "holdings value"]
    for rh in required_headers:
        if rh not in header_map:
            raise Exception(f"Missing expected header: {rh}")

    # Parse each row in the table body
    rows = table.find("tbody").find_all("tr")
    managers = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < len(header_map):
            continue  # Skip malformed or incomplete rows

        try:
            fund_col = cols[header_map["name"]]
            fund_name = fund_col.get_text(strip=True)
            manager_href = fund_col.find("a")["href"]
            manager_url = BASE_URL + manager_href.strip()

            location = cols[header_map["location"]].get_text(strip=True)
            most_recent_filing = cols[header_map["most recent filing"]].get_text(strip=True)
            num_holdings = cols[header_map["num holdings"]].get_text(strip=True)
            holdings_value = cols[header_map["holdings value"]].get_text(strip=True)

            managers.append({
                "fund_name": fund_name,
                "location": location,
                "most_recent_filing": most_recent_filing,
                "num_holdings": num_holdings,
                "holdings_value": holdings_value,
                "manager_url": manager_url
            })
        except Exception as e:
            print(f"[!] Skipped row due to error: {e}")
            continue

    print(f"Fetched {len(managers)} managers.")
    return managers

if __name__ == "__main__":
    managers_data = scrape_all_managers()
    df = pd.DataFrame(managers_data)
    df.to_csv("data/managers.csv", index=False, quotechar='"', quoting=1)
    print("Saved to data/managers.csv")
