import asyncio
import csv
import unicodedata
import os
import pandas as pd
from playwright.async_api import async_playwright

FILINGS_CSV = "data/filings.csv"
OUTPUT_CSV = "data/holdings.csv"
FAILED_CSV = "data/failed_filings.csv"
NO_COM_CSV = "data/no_com_found.csv"
os.makedirs("data", exist_ok=True)

REQUIRED_HEADERS = ["SYM", "ISSUER NAME", "CL", "CUSIP", "VALUE ($000)", "%", "SHARES"]
CHUNK_SIZE = 10
CONCURRENCY_LIMIT = 50
MAX_RETRIES = 2

lock = asyncio.Lock()
first_chunk_written = [False]
progress_counter = {"current": 0}
progress_lock = asyncio.Lock()

def clean(text):
    return unicodedata.normalize("NFKD", text or "").replace("\u200b", "").strip().upper()

def parse_int(text):
    try: return int(text.replace(",", "").strip())
    except: return 0

def parse_float(text):
    try: return float(text.replace(",", "").replace("%", "").strip())
    except: return 0.0

def write_holdings_chunk(chunk, is_first_chunk=False):
    df = pd.DataFrame(chunk)
    df = df.drop_duplicates(subset=["fund_name", "quarter", "stock_symbol"])
    df.to_csv(
        OUTPUT_CSV,
        mode='a',
        header=is_first_chunk,
        index=False,
        quotechar='"',
        quoting=csv.QUOTE_ALL
    )
    print(f"[✓] Wrote {len(df)} holdings to {OUTPUT_CSV}")

def log_no_com_found(filing):
    with open(NO_COM_CSV, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow([filing["fund_name"], filing["quarter"], filing["filing_url"]])

async def safe_goto(page, url):
    for attempt in range(MAX_RETRIES + 1):
        try:
            await page.goto(url, timeout=60000)
            return True
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"[x] Final failure on {url}")
                return False
            print(f"[!] Retry {attempt + 1} for {url}")
            await asyncio.sleep(2)

async def process_filing(browser, filing, semaphore, total_count):
    chunk = []
    try:
        async with semaphore:
            page = await browser.new_page()
            if not await safe_goto(page, filing["filing_url"]):
                await page.close()
                return [], True

            await page.wait_for_selector("table#filingAggregated thead tr th", timeout=10000)
            headers = await page.query_selector_all("table#filingAggregated thead tr th")
            header_map = {clean(await h.inner_text()): idx for idx, h in enumerate(headers)}

            if not all(h in header_map for h in REQUIRED_HEADERS):
                print(f"[!] Missing headers in {filing['filing_url']}")
                await page.close()
                return [], True

            rows = await page.query_selector_all("table#filingAggregated tbody tr")
            for row in rows:
                cols = await row.query_selector_all("td")
                if len(cols) < len(REQUIRED_HEADERS): continue

                cl = clean(await cols[header_map["CL"]].inner_text())
                if cl != "COM": continue

                record = {
                    "fund_name": filing["fund_name"],
                    "filing_date": filing["filing_date"],
                    "quarter": filing["quarter"],
                    "stock_symbol": clean(await cols[header_map["SYM"]].inner_text()),
                    "issuer_name": clean(await cols[header_map["ISSUER NAME"]].inner_text()),
                    "cl": cl,
                    "cusip": clean(await cols[header_map["CUSIP"]].inner_text()),
                    "value_($000)": parse_int(await cols[header_map["VALUE ($000)"]].inner_text()),
                    "shares": parse_int(await cols[header_map["SHARES"]].inner_text()),
                    "shares_type": "SH",
                    "portfolio_percent": parse_float(await cols[header_map["%"]].inner_text()),
                    "filing_url": filing["filing_url"]
                }
                chunk.append(record)

            await page.close()

        if not chunk:
            log_no_com_found(filing)
            return [], False

        async with lock:
            for i in range(0, len(chunk), CHUNK_SIZE):
                subchunk = chunk[i:i + CHUNK_SIZE]
                write_holdings_chunk(subchunk, is_first_chunk=not first_chunk_written[0])
                first_chunk_written[0] = True

        return chunk, False

    except Exception as e:
        print(f"[x] Failed: {filing['fund_name']} | {filing['quarter']} — {e}")
        return [], True

    finally:
        async with progress_lock:
            progress_counter["current"] += 1
            print(f"[→] Progress: {progress_counter['current']} / {total_count}", flush=True)

async def main():
    for f in [OUTPUT_CSV, FAILED_CSV, NO_COM_CSV]:
        if os.path.exists(f): os.remove(f)

    df = pd.read_csv(FILINGS_CSV, quotechar='"', skipinitialspace=True)
    df = df.drop_duplicates(subset=["fund_name", "quarter", "filing_url"])
    total = len(df)

    failed_urls = []
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        tasks = [process_filing(browser, row.to_dict(), semaphore, total) for _, row in df.iterrows()]
        results = await asyncio.gather(*tasks)

        for i, (_, failed) in enumerate(results):
            if failed:
                failed_urls.append(df.iloc[i]["filing_url"])

        await browser.close()

    if failed_urls:
        pd.DataFrame({"failed_urls": failed_urls}).to_csv(FAILED_CSV, index=False)
        print(f"[!] Saved failed filings to {FAILED_CSV}")

if __name__ == "__main__":
    asyncio.run(main())
