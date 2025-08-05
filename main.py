import logging
from pyspark.sql import SparkSession
from src.ingestion.load_raw_data import load_data
from src.transformation.transform_data import transformar_e_gerar_insights, transformar_base
from src.loading.save_data import save_dados_formatados, save_insights
from src.utils.logger import setup_logging  

def main():
    logger = setup_logging()

    logger.info("Iniciando pipeline ETL...")

    spark = SparkSession.builder.appName("ETL_Divida_Publica").getOrCreate()

    df_pandas, df_spark = load_data(spark)
    logger.info("Dados brutos carregados com sucesso.")

    df_formatado = transformar_base(df_spark)
    save_dados_formatados(df_formatado)
    logger.info("Dados formatados salvos em 'data/processed'.")

    df_insights = transformar_e_gerar_insights(df_formatado)
    save_insights(df_insights)
    logger.info("Insights econômicos gerados e salvos em 'data/output'.")

    print("\nExemplos de insights gerados:\n")

    df_insights_lidos = spark.read.csv("data/output/insights_economicos_csv", header=True, inferSchema=True)
    df_top1 = df_insights_lidos.filter("rank_ano = 1")

    for row in df_top1.collect():
        print("-" * 50)
        print(f"Ano de vencimento: {row['vencimento_ano']}")
        print(f"Título: {row['id_divida']} (vencimento: {row['vencimento_formatado']})")
        print(f"Valor do estoque: R$ {float(row['valor_estoque']):,.2f}")
        print(f"Participação no total do ano: {float(row['percentual_contribuicao']):.2f}%")
        print(f"Posição no ranking: {row['rank_ano']}")

    logger.info("Pipeline finalizada com sucesso.")

if __name__ == "__main__":
    main()
