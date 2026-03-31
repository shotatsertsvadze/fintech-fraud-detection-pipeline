import subprocess
from src.utils.logger import get_logger

logger = get_logger(__name__)

def run_step(name, command):
    logger.info(f"Running: {name}")
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        logger.error(f"{name} failed")
        raise Exception(f"{name} failed")
    logger.info(f"{name} completed successfully")

if __name__ == "__main__":
    logger.info("Starting full pipeline")

    run_step("Silver Layer", "python -m src.silver.clean_transactions")
    run_step("Analytics", "python -m src.analytics.run_analytics")
    run_step("Gold Layer", "python -m src.gold.build_daily_summary")

    logger.info("Pipeline finished successfully")