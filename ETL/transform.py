from pyspark.sql.functions import col, trim , to_date


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
