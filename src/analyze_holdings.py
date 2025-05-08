import pandas as pd

INPUT_FILE = "data/holdings.csv"
OUTPUT_FILE = "data/final_com_holdings_analysis.csv"

 # For normalization of quarter format
def normalize_quarter_format(q):
    if isinstance(q, str):
        q = q.strip()
        if q.startswith("Q") and " " in q:
            q_part, y_part = q.split(" ")
            return f"{y_part}Q{q_part[1]}"
        elif "Q" in q and len(q) >= 6 and q[4] == "Q":
            return q 
    return q

def compare_adjacent_quarters(df):
    df["quarter"] = df["quarter"].apply(normalize_quarter_format)
    print("Unique quarters after normalization:", df["quarter"].unique())

    df = df.dropna(subset=["stock_symbol", "shares"])
    df = df[df["shares_type"] == "SH"]
    df["shares"] = df["shares"].astype(int)
    df = df.drop_duplicates(subset=["fund_name", "quarter", "stock_symbol"])
    df = df.sort_values(by=["fund_name", "stock_symbol", "quarter"])

    all_quarters = sorted(df["quarter"].unique())
    first_write = True
    buffer = []

    for (fund, stock), group in df.groupby(["fund_name", "stock_symbol"]):
        group_sorted = group.sort_values("quarter")

        for i in range(1, len(all_quarters)):
            q_prev = all_quarters[i - 1]
            q_curr = all_quarters[i]

            try:
                y_prev, qn_prev = int(q_prev[:4]), int(q_prev[5])
                y_curr, qn_curr = int(q_curr[:4]), int(q_curr[5])
            except:
                continue

            is_adjacent = (y_curr == y_prev and qn_curr - qn_prev == 1) or \
                          (y_curr - y_prev == 1 and qn_prev == 4 and qn_curr == 1)
            if not is_adjacent:
                continue

            prev_row = group_sorted[group_sorted["quarter"] == q_prev]
            curr_row = group_sorted[group_sorted["quarter"] == q_curr]

            if prev_row.empty and curr_row.empty:
                continue
            elif prev_row.empty:
                curr = curr_row.iloc[0]
                row = {
                    "fund_name": curr["fund_name"],
                    "filing_date": curr["filing_date"],
                    "quarter": curr["quarter"],
                    "stock_symbol": curr["stock_symbol"],
                    "cl": curr["cl"],
                    "value_($000)": curr["value_($000)"],
                    "shares": curr["shares"],
                    "change": curr["shares"],
                    "pct_change": 100.0,
                    "inferred_transaction_type": "buy"
                }
            elif curr_row.empty:
                prev = prev_row.iloc[0]
                row = {
                    "fund_name": prev["fund_name"],
                    "filing_date": prev["filing_date"],
                    "quarter": q_curr,
                    "stock_symbol": prev["stock_symbol"],
                    "cl": prev["cl"],
                    "value_($000)": 0,
                    "shares": 0,
                    "change": -prev["shares"],
                    "pct_change": -100.0,
                    "inferred_transaction_type": "sell"
                }
            else:
                prev = prev_row.iloc[0]
                curr = curr_row.iloc[0]
                change = curr["shares"] - prev["shares"]
                pct_change = round((change / prev["shares"]) * 100, 2) if prev["shares"] != 0 else None

                if change > 0:
                    action = "buy"
                elif change < 0:
                    action = "sell"
                else:
                    action = "hold"

                row = {
                    "fund_name": curr["fund_name"],
                    "filing_date": curr["filing_date"],
                    "quarter": curr["quarter"],
                    "stock_symbol": curr["stock_symbol"],
                    "cl": curr["cl"],
                    "value_($000)": curr["value_($000)"],
                    "shares": curr["shares"],
                    "change": change,
                    "pct_change": pct_change,
                    "inferred_transaction_type": action
                }

            buffer.append(row)

            if len(buffer) == 10:
                pd.DataFrame(buffer).to_csv(OUTPUT_FILE, mode="a", index=False, header=first_write)
                print(f"[✓] Wrote 10 rows to {OUTPUT_FILE}")
                buffer.clear()
                first_write = False

    # Write any remaining rows
    if buffer:
        pd.DataFrame(buffer).to_csv(OUTPUT_FILE, mode="a", index=False, header=first_write)
        print(f"[✓] Wrote final {len(buffer)} rows to {OUTPUT_FILE}")


if __name__ == "__main__":
    print("Loading holdings...")
    df = pd.read_csv(INPUT_FILE)

    print("Analyzing...")
    compare_adjacent_quarters(df)

    print("[✓] All comparisons complete.")

