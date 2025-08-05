import pandas as pd
from pyspark.sql import SparkSession

def load_data(spark: SparkSession):
    path = "data/raw/br_me_estoque_divida_publica_microdados.csv"

    df_pandas = pd.read_csv(path, sep=",")
    
    df_spark = spark.read.csv(path, header=True, sep=",", inferSchema=True)

    return df_pandas, df_spark
