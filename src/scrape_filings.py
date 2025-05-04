import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

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
    fund_name = manager["fund_name"]
    manager_url = manager["manager_url"]

    try:
        response = requests.get(manager_url)
        if response.status_code != 200:
            print(f"Failed to load manager page: {manager_url}")
            return filings

        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table")

        if not table:
            print(f"No filings table found for: {fund_name}")
            return filings

        rows = table.find("tbody").find_all("tr")

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 6:
                continue  # Skip malformed rows

            form_type = cols[4].text.strip()
            if form_type != "13F-HR":
                continue  # Only keep 13F-HR filings

            quarter = cols[0].text.strip()
            holdings = cols[1].text.strip()  # optional
            value = cols[2].text.strip()     # optional
            top_holdings = cols[3].text.strip()  # optional
            filing_date = cols[5].text.strip()
            filing_href = cols[0].find("a")["href"]
            filing_url = BASE_URL + filing_href.strip()

            filings.append({
                "fund_name": fund_name,
                "quarter": quarter,
                "filing_date": filing_date,
                "form_type": form_type,
                "filing_url": filing_url
            })

    except Exception as e:
        print(f"Error scraping filings for {fund_name}: {e}")

    return filings


def scrape_all_13fhr_filings():
    """
    Loads all managers and scrapes their 13F-HR filings.
    
    Returns:
        DataFrame: All valid 13F-HR filing metadata across all managers.
    """
    print("Loading manager list...")
    df = pd.read_csv(MANAGERS_CSV)
    all_filings = []

    for i, row in df.iterrows():
        manager = row.to_dict()
        print(f"Scraping filings for: {manager['fund_name']} ({i+1}/{len(df)})")
        filings = scrape_filings_for_manager(manager)
        all_filings.extend(filings)

        # Polite delay to avoid hammering the server
        time.sleep(0.5)

    filings_df = pd.DataFrame(all_filings)
    return filings_df


if __name__ == "__main__":
    filings_df = scrape_all_13fhr_filings()

    output_path = "data/filings.csv"
    filings_df.to_csv(output_path, index=False)
    print(f"Saved {len(filings_df)} filings to {output_path}")
