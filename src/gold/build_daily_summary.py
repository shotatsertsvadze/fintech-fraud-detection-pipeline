import pandas as pd
from pathlib import Path
from src.utils.logger import get_logger
from src.utils.config import load_config

logger = get_logger(__name__)
config = load_config()

SILVER_PATH = Path(config["paths"]["silver"])
GOLD_PATH = Path(config["paths"]["gold"])
GOLD_DIR = GOLD_PATH.parent  # ✅ fix

def main():
    logger.info("Starting gold layer job...")

    if not SILVER_PATH.exists():
        raise FileNotFoundError(f"Missing silver file: {SILVER_PATH}")

    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(SILVER_PATH)

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

    daily_summary.to_parquet(GOLD_PATH, index=False)

    logger.info(f"Gold file created at: {GOLD_PATH}")
    print(daily_summary)

    logger.info("Gold layer job finished.")

if __name__ == "__main__":
    main()