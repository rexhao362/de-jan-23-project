from src.lambdas.ingestion.ingestion import data_ingestion

try:
    load_new_data_into_warehouse_db(test_path)

except Exception as exc:
    msg = f"\nError: {exc}"
    exit(msg)  # replace with log() to CloudWatch

finally:
    pass
    # clean up