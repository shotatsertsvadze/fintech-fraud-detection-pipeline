import pandas as pd
from pathlib import Path
from src.utils.logger import get_logger

logger = get_logger(__name__)

SILVER_PATH = Path("data/silver/transactions_clean.csv")

def main():
    logger.info("Starting analytics job...")

    df = pd.read_csv(SILVER_PATH)

    total_transactions = len(df)
    total_revenue = df["amount"].sum()
    fraud_transactions = (df["fraud_flag"] == 1).sum()
    fraud_rate = fraud_transactions / total_transactions if total_transactions else 0

    logger.info(f"Total transactions: {total_transactions}")
    logger.info(f"Total revenue: {total_revenue}")
    logger.info(f"Fraud transactions: {fraud_transactions}")
    logger.info(f"Fraud rate: {fraud_rate:.2%}")

    revenue_by_country = (
        df.groupby("country", as_index=False)["amount"]
        .sum()
        .sort_values("amount", ascending=False)
    )

    high_risk = df[df["risk_level"] == "high"]

    # Keep prints for readable output
    print("\n=== Analytics Summary ===")
    print(f"Total transactions: {total_transactions}")
    print(f"Total revenue: {total_revenue}")
    print(f"Fraud transactions: {fraud_transactions}")
    print(f"Fraud rate: {fraud_rate:.2%}")

    print("\n=== Revenue by Country ===")
    print(revenue_by_country.to_string(index=False))

    print("\n=== High Risk Transactions ===")
    if high_risk.empty:
        print("No high risk transactions found.")
    else:
        print(high_risk.to_string(index=False))

    logger.info("Analytics job finished.")


if __name__ == "__main__":
    main()