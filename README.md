# 13F-HR Scraper

This project scrapes 13F-HR filings from https://13f.info/, extracts COM-class stock holdings, and compares adjacent quarterly filings to infer whether a transaction is a "buy", "sell", or "hold".

# Output Format

The final CSV includes the following columns:

| fund_name | filing_date | quarter | stock_symbol | cl | value_($000) | shares | change | pct_change | inferred_transaction_type |

# Transaction Inference Rules

- If current quarter shares > previous quarter → "buy"
- If current quarter shares < previous quarter → "sell"
- If shares are equal → "hold"
- If a stock appears only in the current quarter → "buy"
- If a stock disappears in the current quarter → "sell"
