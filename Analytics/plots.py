import matplotlib.pyplot as plt
from pyspark.sql.functions import sum as spark_sum

def plot_profit_by_category(df, col_name_):
    pdf = (
        df.groupBy(col_name_)
        .agg(spark_sum("Profit").alias("TotalProfit"))
        .toPandas()
    )

    plt.figure(figsize=(10, 6))
    plt.bar(pdf[col_name_], pdf["TotalProfit"], color='skyblue')
    plt.xticks(rotation=80)
    plt.xlabel(col_name_)
    plt.ylabel("Total Profit")
    plt.title(f"Total Profit by {col_name_}")
    plt.tight_layout()
    plt.show()