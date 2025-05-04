import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os

FILINGS_CSV = "data/filings.csv"
OUTPUT_CSV = "data/holdings.csv"
FAILED_CSV = "data/failed_filings.csv"

def setup_driver():
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--log-level=3') 
    options.add_argument('--disable-logging')
    options.add_argument('--blink-settings=imagesEnabled=false')
    return webdriver.Chrome(options=options)

def scrape_holdings_for_filing(driver, filing):
    holdings = []

    try:
        driver.get(filing["filing_url"])
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
        )

        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) < 7:
                continue

            cl = cols[2].text.strip().upper()
            if "COM" not in cl:
                continue

            symbol = cols[0].text.strip()
            issuer_name = cols[1].text.strip()
            cusip = cols[3].text.strip()
            value = cols[4].text.strip().replace(",", "")
            shares_raw = cols[5].text.strip()

            # Skip rows with % shares
            if "%" in shares_raw:
              print(f"[✓] Capturing % shares row: value={value}, shares={shares_raw}")
              shares = shares_raw.replace("%", "").strip()
              shares_type = "percent"
            else:
                shares = shares_raw.replace(",", "").strip()
                shares_type = "count"

            try:
                value = int(value)
            except ValueError:
                print(f"[!] Skipping malformed value: {value}")
                continue

            holdings.append({
                "fund_name": filing["fund_name"],
                "filing_date": filing["filing_date"],
                "quarter": filing["quarter"],
                "stock_symbol": symbol,
                "issuer_name": issuer_name,
                "cl": cl,
                "cusip": cusip,
                "value_($000)": value,
                "shares": shares,
                "shares_type": shares_type,
                "filing_url": filing["filing_url"]
            })


        if not holdings:
            print(f"[!] No COM holdings found for: {filing['fund_name']} | {filing['quarter']}")

    except Exception as e:
        print(f"[x] Failed to scrape {filing['fund_name']} {filing['quarter']}: {e}")
        raise

    return holdings

def scrape_all_com_holdings():
    print("Loading filings...")
    df = pd.read_csv(FILINGS_CSV)

    if df.empty:
        print("[!] No filings to process.")
        return pd.DataFrame()

    before = len(df)
    df = df.drop_duplicates(subset=["fund_name", "quarter", "filing_url"])
    after = len(df)
    print(f"Removed {before - after} duplicate filings.")

    all_holdings = []
    failed_urls = []

    driver = setup_driver()

    for i, row in df.iterrows():
        filing = row.to_dict()
        print(f"[→] Scraping: {filing['fund_name']} | {filing['quarter']} ({i+1}/{len(df)})")

        try:
            holdings = scrape_holdings_for_filing(driver, filing)
            all_holdings.extend(holdings)
        except Exception:
            failed_urls.append(filing["filing_url"])

        time.sleep(0.1)

    driver.quit()

    if failed_urls:
        os.makedirs("data", exist_ok=True)
        pd.DataFrame({"failed_urls": failed_urls}).to_csv(FAILED_CSV, index=False)
        print(f"[!] Saved failed filings to {FAILED_CSV}")

    holdings_df = pd.DataFrame(all_holdings)
    holdings_df = holdings_df.drop_duplicates(subset=["fund_name", "quarter", "stock_symbol"])

    return holdings_df

if __name__ == "__main__":
    holdings_df = scrape_all_com_holdings()
    if not holdings_df.empty:
        holdings_df.to_csv(OUTPUT_CSV, index=False)
        print(f"[✓] Saved {len(holdings_df)} COM-class holdings to {OUTPUT_CSV}")
    else:
        print("[!] No holdings found.")
