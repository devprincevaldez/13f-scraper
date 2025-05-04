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

    managers = []

    # Parse each row in the table body
    rows = table.find("tbody").find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        if len(cols) != 5:
            continue  # Skip malformed or incomplete rows

        # Extract manager name and profile URL
        fund_name = cols[0].text.strip()
        manager_href = cols[0].find("a")["href"]
        manager_url = BASE_URL + manager_href.strip()

        # Extract other metadata
        location = cols[1].text.strip()
        most_recent_filing = cols[2].text.strip()
        num_holdings = cols[3].text.strip()
        holdings_value = cols[4].text.strip()

        managers.append({
            "fund_name": fund_name,
            "location": location,
            "most_recent_filing": most_recent_filing,
            "num_holdings": num_holdings,
            "holdings_value": holdings_value,
            "manager_url": manager_url
        })

    print(f"Fetched {len(managers)} managers.")
    return managers


if __name__ == "__main__":
    # Run the scraper and save results to CSV
    managers_data = scrape_all_managers()
    df = pd.DataFrame(managers_data)
    df.to_csv("data/managers.csv", index=False)
    print("Saved to data/managers.csv")
