import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
import time
import os
import re
import unicodedata
import csv

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
    options.add_argument('--enable-unsafe-swiftshader')  
    return webdriver.Chrome(options=options)

def scrape_holdings_for_filing(driver, filing):
    holdings = []

    try:
        driver.get(filing["filing_url"])
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table thead tr th"))
        )
        time.sleep(1) 


        headers = driver.find_elements(By.CSS_SELECTOR, "table thead tr th")
        header_titles = [h.get_attribute("textContent").strip().upper() for h in headers]

        header_indices = {title: idx for idx, title in enumerate(header_titles)}
        required_headers = ["SYM", "ISSUER NAME", "CL", "CUSIP", "VALUE ($000)", "%", "SHARES"]
        missing_headers = [h for h in required_headers if h not in header_indices]
        if missing_headers:
            print(f"[!] Missing columns in {filing['filing_url']}: {missing_headers}")
            return holdings

        cl_index = header_indices["CL"]
        symbol_index = header_indices["SYM"]
        issuer_index = header_indices["ISSUER NAME"]
        cusip_index = header_indices["CUSIP"]
        value_index = header_indices["VALUE ($000)"]
        percent_index = header_indices["%"]
        shares_index = header_indices["SHARES"]

        def clean_text(text):
            if not text:
                return ""
            return unicodedata.normalize("NFKD", text).replace("\u200b", "").strip().upper()

        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) <= max(cl_index, symbol_index, issuer_index, cusip_index, value_index, percent_index, shares_index):
                continue

            try:
                cl_cell = cols[cl_index]
                cl = ""
                try:
                    cl = clean_text(cl_cell.find_element(By.TAG_NAME, "span").text)
                    if not cl:
                        cl = clean_text(cl_cell.find_element(By.TAG_NAME, "span").get_attribute("textContent"))
                except:
                    cl = clean_text(cl_cell.get_attribute("textContent"))

                with open("data/cl_debug_log.csv", "a", encoding="utf-8") as log_file:
                    log_file.write(f"{filing['fund_name']},{filing['quarter']},'{cl}'\n")

                if cl != "COM":
                    continue  # skip non-COM rows

                symbol = clean_text(cols[symbol_index].text)
                issuer_name = clean_text(cols[issuer_index].text)
                cusip = clean_text(cols[cusip_index].text)
                value_raw = cols[value_index].text.strip().replace(",", "")
                percent_raw = cols[percent_index].text.strip().replace("%", "").strip()
                shares_raw = cols[shares_index].text.strip().replace(",", "")

                value = int(value_raw)
                shares = int(shares_raw)
                portfolio_percent = float(percent_raw)

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
                    "shares_type": "SH",
                    "portfolio_percent": portfolio_percent,
                    "filing_url": filing["filing_url"]
                })

            except Exception as e:
                print(f"[!] Skipping malformed row for {filing['filing_url']}: {e}")
                continue

        if not holdings:
            print(f"[!] No COM holdings found for: {filing['fund_name']} | {filing['quarter']}")
            with open("data/no_com_found.csv", "a", encoding="utf-8") as f:
                f.write(f"\"{filing['fund_name']}\",\"{filing['quarter']}\",\"{filing['filing_url']}\"\n")


    except Exception as e:
        print(f"[x] Failed to scrape {filing['fund_name']} {filing['quarter']}: {e}")
        with open("data/no_com_found.csv", "a", encoding="utf-8") as f:
            f.write(f"\"{filing['fund_name']}\",\"{filing['quarter']}\",\"{filing['filing_url']}\"\n")
        raise

    return holdings

def scrape_all_com_holdings():
    print("Loading filings...")
    df = pd.read_csv(FILINGS_CSV, quotechar='"', skipinitialspace=True)

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

def write_holdings_chunk(chunk, is_first_chunk=False):
    if chunk:
        df_chunk = pd.DataFrame(chunk)
        df_chunk = df_chunk.drop_duplicates(subset=["fund_name", "quarter", "stock_symbol"])
        df_chunk.to_csv(OUTPUT_CSV, mode='a', header=is_first_chunk, index=False, quotechar='"', quoting=csv.QUOTE_ALL)
        print(f"[✓] Wrote {len(df_chunk)} holdings to {OUTPUT_CSV}")

if __name__ == "__main__":
    if os.path.exists(OUTPUT_CSV):
        os.remove(OUTPUT_CSV)

    df = pd.read_csv(FILINGS_CSV, quotechar='"', skipinitialspace=True)
    df = df.drop_duplicates(subset=["fund_name", "quarter", "filing_url"])

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

        if (i + 1) % 10 == 0:
            write_holdings_chunk(all_holdings, is_first_chunk=(i < 10))
            all_holdings.clear()

        time.sleep(0.1)

    if all_holdings:
        write_holdings_chunk(all_holdings, is_first_chunk=(len(df) <= 10))

    driver.quit()

    if failed_urls:
        os.makedirs("data", exist_ok=True)
        pd.DataFrame({"failed_urls": failed_urls}).to_csv(FAILED_CSV, index=False)
        print(f"[!] Saved failed filings to {FAILED_CSV}")