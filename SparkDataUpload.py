from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date, sum as spark_sum
from pyspark.sql.functions import expr
import psycopg2
from datetime import datetime
import os
import glob
import matplotlib.pyplot as plt
from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------------------------
# Spark Session (Local)
# -----------------------------------------------------------
spark = SparkSession.builder \
    .appName("Local ETL with PySpark") \
    .master("local[*]") \
    .getOrCreate()

# -----------------------------------------------------------
# DB CONFIG
# -----------------------------------------------------------
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# -----------------------------------------------------------
# CONFIG
# -----------------------------------------------------------
REQUIRED_COLUMNS = [
    "OrderID", "OrderDate", "UnitCost", "Price", "OrderQty",
    "CostOfSales", "Sales", "Profit", "ProductName",
    "Manufacturer", "Country"
]

INSERT_QUERY = """ 
INSERT INTO orderhistory (
    orderid, orderdate, unitcost, price, orderqty,
    costofsales, sales, profit, channel, promotionname,
    productname, manufacturer, productsubcategory, productcategory,
    region, city, country
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (orderid) DO NOTHING;
"""

INSERT_QUERY_REJECTED = """ 
INSERT INTO rejecteddata (
    orderid, orderdate, unitcost, price, orderqty,
    costofsales, sales, profit, channel, promotionname,
    productname, manufacturer, productsubcategory, productcategory,
    region, city, country
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

# -----------------------------------------------------------
# 1. Extract
# -----------------------------------------------------------
def load_file(file_path: str):
    return spark.read.option("header", True).csv(file_path)

# -----------------------------------------------------------
# 2. Transform
# -----------------------------------------------------------
def clean_data(df):
    # Strip whitespace from column names
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())

    # Trim string columns
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))

    #Convert date
    df = df.withColumn(
        "OrderDate",
        to_date(col("OrderDate"), 'M/d/yyyy')
    )


    return df


def is_valid_row(index, row, log_file) -> bool:
    for col_name in REQUIRED_COLUMNS:
        if col_name not in row.asDict():
            log_file.write(f"[MISSING] Row {index} missing column {col_name}\n")
            return False

        value = row[col_name]

        if value is None:
            log_file.write(f"[MISSING] Row {index} NULL value in {col_name}\n")
            return False

        if isinstance(value, str) and value.strip().lower() in ("", "nan", "none"):
            log_file.write(f"[MISSING] Row {index} empty value in {col_name}\n")
            return False

    return True

def empty_to_null(df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(
                c,
                expr(f"NULLIF(TRIM({c}), '')")
            )
    return df

# -----------------------------------------------------------
# 3. Database
# -----------------------------------------------------------
def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def setup_tables():
    conn = get_db_connection()
    cursor = conn.cursor()

    print("Dropping existing tables if any...")    
    cursor.execute("DROP TABLE orderhistory;")
    cursor.execute("DROP TABLE rejecteddata;")


    print("Creating orderhistory table...")
    cursor.execute("""
            CREATE TABLE orderhistory (
            OrderID INT PRIMARY KEY NOT NULL,
            OrderDate DATE NOT NULL,
            UnitCost DECIMAL(18,8) NOT NULL,
            Price DECIMAL(12,2) NOT NULL,
            OrderQty INT NOT NULL,
            CostOfSales DECIMAL(18,8) NOT NULL,
            Sales DECIMAL(14,2) NOT NULL,
            Profit DECIMAL(18,8) NOT NULL,
            Channel VARCHAR(150),
            PromotionName VARCHAR(150),
            ProductName VARCHAR(150) NOT NULL,
            Manufacturer VARCHAR(150) NOT NULL,
            ProductSubCategory VARCHAR(150),
            ProductCategory VARCHAR(150),
            Region VARCHAR(150),
            City VARCHAR(150),
            Country VARCHAR(150) NOT NULL
        );
    """)
    #cursor.execute("""Insert into orderhistory (OrderID, OrderDate, UnitCost, Price, OrderQty, CostOfSales,
    #               Sales, Profit, Channel, PromotionName, ProductName, Manufacturer, ProductSubCategory,
    #                ProductCategory, Region, City, Country) Values
    #                (5, '2024-01-01', 10.00, 12.00, 1, 10.00, 12.00,
    #                2.00, 'Online', 'New Year Promo', 'Gadget Pro',
    #                'TechCorp', 'Gadgets', 'Electronics', 'North America',
    #                'New York', 'USA');
    #""")

    print("Creating rejecteddata table...")
    cursor.execute("""
        CREATE TABLE rejecteddata (
            OrderID INT,
            OrderDate DATE,
            UnitCost DECIMAL(18,8),
            Price DECIMAL(12,2),
            OrderQty INT,
            CostOfSales DECIMAL(18,8),
            Sales DECIMAL(14,2),
            Profit DECIMAL(18,8) ,
            Channel VARCHAR(150),
            PromotionName VARCHAR(150),
            ProductName VARCHAR(150),
            Manufacturer VARCHAR(150),
            ProductSubCategory VARCHAR(150),
            ProductCategory VARCHAR(150),
            Region VARCHAR(150),
            City VARCHAR(150),
            Country VARCHAR(150)
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()


# -----------------------------------------------------------
# 4. Load
# -----------------------------------------------------------
def insert_row(row, log_file):

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        INSERT INTO orderhistory (
            orderid, orderdate, unitcost, price, orderqty,
            costofsales, sales, profit, channel, promotionname,
            productname, manufacturer, productsubcategory, productcategory,
            region, city, country
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (orderid) DO NOTHING;
        """, (
            int(row.OrderID),
            row.OrderDate,
            float(row.UnitCost),
            float(row.Price),
            float(row.OrderQty),
            float(row.CostOfSales),
            float(row.Sales),
            float(row.Profit),
            row.Channel,
            row.PromotionName,
            row.ProductName,
            row.Manufacturer,
            row.ProductSubCategory,
            row.ProductCategory,
            row.Region,
            row.City,
            row.Country
        ))

        if cursor.rowcount == 0:
            log_file.write(f"[DUPLICATE] OrderID {row.OrderID}\n")
            insert_rejected_row(row, log_file)
            conn.commit()
            cursor.close()
            conn.close()
            return "duplicate"

        conn.commit()
        cursor.close()
        conn.close()
        return "inserted"

    except Exception as e:
        cursor.connection.rollback()
        log_file.write(f"[ERROR] OrderID {row.OrderID}: {e}\n")

        conn.commit()
        cursor.close()
        conn.close()
        return "error"


def insert_rejected_row(row, log_file):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        INSERT INTO rejecteddata (
            orderid, orderdate, unitcost, price, orderqty,
            costofsales, sales, profit, channel, promotionname,
            productname, manufacturer, productsubcategory, productcategory,
            region, city, country
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """, (
            int(row.OrderID),
            row.OrderDate,
            float(row.UnitCost),
            float(row.Price),
            float(row.OrderQty),
            float(row.CostOfSales),
            float(row.Sales),
            float(row.Profit),
            row.Channel,
            row.PromotionName,
            row.ProductName,
            row.Manufacturer,
            row.ProductSubCategory,
            row.ProductCategory,
            row.Region,
            row.City,
            row.Country
        ))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        cursor.connection.rollback()
        conn.commit()
        cursor.close()
        conn.close()
        log_file.write(f"[REJECTED TABLE ERROR] {e}\n")

# -----------------------------------------------------------
# 5. Log Cleanup
# -----------------------------------------------------------
def cleanup_old_logs(max_logs=8):
    logs = sorted(
        glob.glob("insert_log_*.txt"),
        key=os.path.getmtime,
        reverse=True
    )
    for old in logs[max_logs:]:
        os.remove(old)

# -----------------------------------------------------------
# 6. Main
# -----------------------------------------------------------
def main():
    df = load_file("Dataset50.csv")

    df = clean_data(df)

    df = empty_to_null(df)

    log_filename = f"insert_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    log_file = open(log_filename, "w", encoding="utf-8")


    setup_tables()

    conn = get_db_connection()
    cursor = conn.cursor()

    inserted = skipped_conflicts = skipped_errors = 0

    for row in df.toLocalIterator():

        result = insert_row(row, log_file)
        if result == "duplicate":
            skipped_conflicts += 1
        elif result == "error":
            skipped_errors += 1
        else:
            inserted += 1
            

    conn.commit()
    cursor.close()
    conn.close()
    log_file.close()
    cleanup_old_logs()

    print("Inserted:", inserted)
    print("Rejected:", skipped_conflicts + skipped_errors)

    # ------------------------------------
    # Plotting (Spark → Pandas)
    # ------------------------------------
    categories = ["Channel", "Manufacturer", "Region", "City", "Country"]

    while True:
        print(categories)
        user = input("Which category? (q to quit): ").title().strip()
        if user.lower() == "q":
            break
        if user not in categories:
            print("Invalid category")
            continue

        plot_profit_by_category(df, user)
        break


def plot_profit_by_category(df, col_name):
    pdf = (
        df.groupBy(col_name)
        .agg(spark_sum("Profit").alias("TotalProfit"))
        .orderBy(col("TotalProfit").desc())
        .toPandas()
    )

    plt.figure(figsize=(10, 6))
    plt.bar(pdf[col_name], pdf["TotalProfit"])
    plt.xticks(rotation=80)
    plt.title(f"Total Profit by {col_name}")
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
