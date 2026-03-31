import pandas as pd
from pathlib import Path
from src.utils.logger import get_logger

logger = get_logger(__name__)

RAW_PATH = Path("data/raw/transactions_raw.csv")
SILVER_DIR = Path("data/silver")
SILVER_PATH = SILVER_DIR / "transactions_clean.csv"

REQUIRED_COLUMNS = [
    "transaction_id",
    "user_id",
    "timestamp",
    "amount",
    "country",
    "status",
]

STATUS_MAPPING = {
    "approved": "approved",
    "completed": "approved",
    "success": "approved",
    "successful": "approved",
    "paid": "approved",
    "declined": "declined",
    "failed": "declined",
    "rejected": "declined",
    "denied": "declined",
    "pending": "pending",
    "processing": "pending",
    "in_progress": "pending",
}

VALID_STATUS = {"approved", "declined", "pending"}
SUSPICIOUS_COUNTRIES = {"NG", "RU"}


def validate_schema(df: pd.DataFrame) -> None:
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")


def normalize_schema(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    string_cols = ["transaction_id", "user_id", "country", "status"]
    for col in string_cols:
        df[col] = df[col].astype("string").str.strip()

    for col in string_cols:
        df[col] = df[col].replace(
            {"": pd.NA, "nan": pd.NA, "None": pd.NA, "NONE": pd.NA, "NAN": pd.NA}
        )

    df["transaction_id"] = df["transaction_id"].str.upper()
    df["user_id"] = df["user_id"].str.upper()
    df["country"] = df["country"].str.upper()
    df["status"] = df["status"].str.lower()

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    return df


def normalize_status_values(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    logger.info("Status values before mapping:")
    print(df["status"].value_counts(dropna=False))

    df["status"] = df["status"].replace(STATUS_MAPPING)

    logger.info("Status values after mapping:")
    print(df["status"].value_counts(dropna=False))

    return df


def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    rows_start = len(df)

    validate_schema(df)
    df = normalize_schema(df)
    df = normalize_status_values(df)

    before = len(df)
    df = df.dropna(subset=["transaction_id", "user_id", "timestamp", "amount", "country", "status"])
    logger.info(f"Rows removed due to nulls/invalid parsing: {before - len(df)}")

    before = len(df)
    df = df[df["amount"] > 0]
    logger.info(f"Rows removed due to invalid amount: {before - len(df)}")

    before = len(df)
    df = df[df["country"].str.len() == 2]
    logger.info(f"Rows removed due to invalid country code: {before - len(df)}")

    before = len(df)
    df = df[df["status"].isin(VALID_STATUS)]
    logger.info(f"Rows removed due to invalid status: {before - len(df)}")

    before = len(df)
    df = df.drop_duplicates(subset=["transaction_id"], keep="first")
    logger.info(f"Duplicate transaction_id rows removed: {before - len(df)}")

    df["transaction_date"] = df["timestamp"].dt.normalize()

    rows_end = len(df)
    logger.info(f"Total rows removed during cleaning: {rows_start - rows_end}")

    return df


def add_fraud_logic(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["high_amount_flag"] = df["amount"] > 5000

    tx_counts = df["user_id"].value_counts()
    df["many_transactions_flag"] = df["user_id"].map(tx_counts) > 2

    df["suspicious_country_flag"] = df["country"].isin(SUSPICIOUS_COUNTRIES)

    df["fraud_score"] = 0
    df.loc[df["high_amount_flag"], "fraud_score"] += 50
    df.loc[df["many_transactions_flag"], "fraud_score"] += 30
    df.loc[df["suspicious_country_flag"], "fraud_score"] += 40

    df["fraud_flag"] = (df["fraud_score"] >= 50).astype(int)

    def assign_risk_level(score: int) -> str:
        if score >= 80:
            return "high"
        elif score >= 50:
            return "medium"
        return "low"

    df["risk_level"] = df["fraud_score"].apply(assign_risk_level)

    return df


def run_data_quality_checks(df: pd.DataFrame) -> None:
    assert df["transaction_id"].is_unique, "Duplicate transaction IDs found!"
    assert (df["amount"] > 0).all(), "Invalid amounts found!"
    assert df["status"].isin(["approved", "declined", "pending"]).all(), "Invalid status values found!"


def main():
    logger.info("Starting silver layer job...")

    if not RAW_PATH.exists():
        raise FileNotFoundError(f"Missing raw file: {RAW_PATH}")

    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(RAW_PATH)
    logger.info(f"Rows before cleaning: {len(df)}")

    df = clean_transactions(df)
    df = add_fraud_logic(df)

    run_data_quality_checks(df)

    logger.info(f"Rows after cleaning: {len(df)}")

    print("\nPreview of cleaned data:")
    print(df.head(10))

    print("\nData types:")
    print(df.dtypes)

    df.to_csv(SILVER_PATH, index=False)
    logger.info(f"Silver file created at: {SILVER_PATH}")


if __name__ == "__main__":
    main()