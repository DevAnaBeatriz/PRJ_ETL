from pyspark.sql import DataFrame
import pandas as pd

def save_dados_formatados(df: DataFrame):
    """Salva os dados tratados na pasta 'processed' ."""
    
    df.coalesce(1).write.mode("overwrite").option("header", True).csv("data/processed/base_formatada_csv")
    df.coalesce(1).write.mode("overwrite").parquet("data/processed/base_formatada_parquet")


def save_insights(df: DataFrame):
    """Salva os dados analíticos na pasta 'output'."""
    df.coalesce(1).write.mode("overwrite").option("header", True).csv("data/output/insights_economicos_csv")
    df.coalesce(1).write.mode("overwrite").parquet("data/output/insights_economicos_parquet")
