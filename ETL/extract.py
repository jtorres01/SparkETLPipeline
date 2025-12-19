

def load_file(spark,file_path: str):
    return spark.read.option("header", True).csv(file_path)
