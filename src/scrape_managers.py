import requests
from bs4 import BeautifulSoup
import pandas as pd
import string

BASE_URL = "https://13f.info"
MANAGER_PATHS = list(string.ascii_lowercase) + ["0"]  
MANAGER_URLS = [f"{BASE_URL}/managers/{letter}" for letter in MANAGER_PATHS]

def scrape_manager_page(url):
    print(f"[→] Scraping: {url}")
    response = requests.get(url)
    if response.status_code != 200:
        print(f"[!] Failed to load {url} (status {response.status_code})")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")

    if not table:
        print(f"[!] No table found at {url}")
        return []

    headers = table.find("thead").find_all("th")
    header_map = {th.get_text(strip=True).lower(): idx for idx, th in enumerate(headers)}

    required_headers = ["name", "location", "most recent filing", "num holdings", "holdings value"]
    if not all(h in header_map for h in required_headers):
        print(f"[!] Missing required headers at {url}")
        return []

    rows = table.find("tbody").find_all("tr")
    managers = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < len(header_map):
            continue

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

    return managers

if __name__ == "__main__":
    all_managers = []

    for url in MANAGER_URLS:
        managers = scrape_manager_page(url)
        all_managers.extend(managers)

    df = pd.DataFrame(all_managers)
    df.to_csv("data/managers.csv", index=False, quotechar='"', quoting=1)
    print(f"[✓] Saved {len(df)} managers to data/managers.csv")
