p13F-HR Holdings Analyzer

Project Description:

- This project scrapes institutional 13F-HR filings from 13f.info, extracts COM-class stock holdings, and analyzes quarterly changes to infer transaction activity (buy, sell, or hold) for each fund-stock pair.

Features

- Scrapes all managers listed on 13f.info
- For each manager, scrapes all 13F-HR filings
- Extracts only COM-class stock holdings
- Dynamically compares adjacent quarterly holdings
- Infers transaction types: buy, sell, or hold
- Outputs a single clean CSV, ready for analysis or visualization

Output Format

- The final CSV (final_com_holdings_analysis.csv) includes the following columns:
  - fund_name [Name of the fund manager]
  - filing_date [SEC filing date for the quarter]
  - quarter [Normalized format: YYYYQn (e.g., 2024Q4)]
  - stock_symbol [Ticker symbol]
  - cl [Class of shares (only COM included)]
  - value\_($000) [Reported value in thousands of dollars]
  - shares [Number of shares held]
  - change [Difference in shares from the previous adjacent quarter]
  - pct_change [Percent change in shares compared to the previous quarter]
  - inferred_transaction_type [One of: buy_new, buy_additional, sell_partial, sell_full, hold]

Transaction Inference Logic

The logic for inferring transaction type follows:

- `buy_new`: New position — stock wasn't held in the prior quarter
- `buy_additional`: Increased an existing holding
- `sell_partial`: Trimmed but retained some shares
- `sell_full`: Fully exited — holding dropped to 0 or stock disappeared
- `hold`: No change in position size

Calculation logic:

=> change = shares_current - shares_previous
=> pct_change = (change / shares_previous) \* 100 # unless shares_previous == 0

Note: Initial buys with no prior data use pct_change = 100.0.

Engineering Considerations

Data Quality and Parsing

- Quoted Fields: All CSV fields are explicitly quoted to prevent parsing errors from commas in names (e.g., "York Capital Management Global Advisors, LLC").
- Strict COM Filtering: Only rows where the class field is exactly "COM" are included. Variants like "COM CL A" are excluded per specification.
- Dynamic Header Mapping: Columns are accessed by header name, not fixed index, to ensure compatibility with varying table formats across filings.
- Quarter Normalization: Formats like "Q3 2024" are automatically normalized to "2024Q3" for accurate chronological comparisons.

Scalability and Robustness

- Results are written in chunks of 10 to avoid memory overload and allow real-time monitoring.
- All data pipelines are deduplicated by fund_name, quarter, and stock_symbol to prevent duplicate entries in the final output.

Repository Structure

```plaintext
project/
├── data/
│   ├── managers.csv                      # All scraped manager metadata
│   ├── filings.csv                       # All 13F-HR filings metadata
│   ├── holdings.csv                      # All raw COM-class holdings
│   ├── final_com_holdings_analysis.csv   # Final output with transaction inference
│   ├── no_com_found.csv                  # Log of filings without COM-class holdings
│   └── cl_debug_log.csv                  # Log of all filings, for debugging purposes (if needed)
├── src/
│   ├── scrape_managers.py                # Manager list scraper
│   ├── scrape_filings.py                 # 13F-HR filings scraper
│   ├── scrape_holdings.py                # COM-class holdings scraper
│   └── analyze_holdings.py               # Core analysis and CSV writer

Getting Started

- Install dependencies (requests, BeautifulSoup, selenium, pandas) and run the following scripts in sequence:

1. python src/scrape_managers.py
2. python src/scrape_filings.py
3. python src/scrape_holdings.py
4. python src/analyze_holdings.py

The final output will be saved to data/final_com_holdings_analysis.csv.

Notes
- Built and tested against over 57,000 fund-stock-quarter combinations.
- Designed for precision, reliability, and transparency.
- Matches the exact format and logic shown in the sample task provided.
```