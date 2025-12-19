from pyspark.sql import SparkSession
from ETL.extract import load_file
from ETL.transform import clean_data
from ETL.load import insert_row, insert_rejected_row
from DataBase.schema import setup_tables
from Logs.logging import create_log, cleanup_old_logs

spark = (SparkSession.builder
         .appName("Spark_ETL_Pipeline")
         .master("local[*]")
         .getOrCreate()
)

def main():
    df = load_file(spark, "Dataset50.csv")
    df = clean_data(df)

    setup_tables()

    log_file = create_log()

    inserted = skipped_conflicts = skipped_errors = 0

    for row in df.toLocalIterator():
        result = insert_row(row)
        if result == "duplicate":
            insert_rejected_row(row)
            log_file.write(f"[DUPLICATE] OrderID {row.OrderID}\n")
            skipped_conflicts += 1
        elif result == "error":
            insert_rejected_row(row)
            skipped_errors += 1
            log_file.write(f"[ERROR] OrderID {row.OrderID}\n")
        else:
            inserted += 1
    

    print("Inserted:", inserted)
    print("Conflicts:", skipped_conflicts)
    print("Errors:", skipped_errors)
    cleanup_old_logs()
    log_file.close()
    spark.stop()

if __name__ == "__main__":
    main()
