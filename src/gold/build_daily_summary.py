import pandas as pd
from pathlib import Path
from src.utils.logger import get_logger

logger = get_logger(__name__)

SILVER_PATH = Path("data/silver/transactions_clean.csv")
GOLD_DIR = Path("data/gold")
GOLD_PATH = GOLD_DIR / "daily_fraud_summary.csv"

def main():
    logger.info("Starting gold layer job...")

    if not SILVER_PATH.exists():
        raise FileNotFoundError(f"Missing silver file: {SILVER_PATH}")

    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(SILVER_PATH, parse_dates=["transaction_date"])

    daily_summary = (
        df.groupby("transaction_date", as_index=False)
        .agg(
            total_transactions=("transaction_id", "count"),
            total_revenue=("amount", "sum"),
            fraud_transactions=("fraud_flag", "sum"),
            fraud_revenue=("amount", lambda x: x[df.loc[x.index, "fraud_flag"] == 1].sum()),
        )
    )

    daily_summary["fraud_rate"] = (
        daily_summary["fraud_transactions"] / daily_summary["total_transactions"]
    )

    daily_summary.to_csv(GOLD_PATH, index=False)

    logger.info(f"Gold file created at: {GOLD_PATH}")
    print(daily_summary)

    logger.info("Gold layer job finished.")

if __name__ == "__main__":
    main()