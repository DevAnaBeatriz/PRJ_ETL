from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from pyspark.sql.functions import (
    col, concat_ws, lpad, sum, row_number, desc,
    year, month, when, trim, date_format, to_date, lit
)

def transformar_base(df: DataFrame) -> DataFrame:
    df = df.withColumn("valor_estoque", col("valor_estoque").cast("double")) \
           .withColumn("quantidade_estoque", col("quantidade_estoque").cast("int")) \
           .withColumn("ano", col("ano").cast("int")) \
           .withColumn("mes", col("mes").cast("int"))

    df = df.filter((col("valor_estoque").isNotNull()) & 
                   (col("valor_estoque") > 0) &
                   (col("quantidade_estoque") > 0))

    df = df.withColumn("data_mes_referencia", to_date(concat_ws("-", col("ano"), lpad(col("mes"), 2, "0"), lit("01"))))
    
    df = df.withColumn("ano_mes", date_format(col("data_mes_referencia"), "dd/MM/yyyy"))

    df = df.withColumn("vencimento_formatado", date_format(col("vencimento_divida"), "dd/MM/yyyy"))

    df = df.withColumn("vencimento_ano", year("vencimento_divida")) \
           .withColumn("vencimento_mes", month("vencimento_divida"))

    df = df.withColumn("tipo_divida", trim(col("tipo_divida"))) \
           .withColumn("classe_carteira", trim(col("classe_carteira"))) \
           .withColumn("tipo_divida", when(col("tipo_divida") == "Divida interna", "Dívida interna").otherwise(col("tipo_divida")))

    df = df.drop("ano", "mes", "data_mes_referencia")

    return df

def transformar_e_gerar_insights(df: DataFrame) -> DataFrame:
    """
    1. Filtra os títulos da dívida interna do mercado com vencimento a partir de 2018.
    2. Calcula o total do estoque da dívida por ano de vencimento.
    3. Calcula o percentual de contribuição de cada título no total anual.
    4. Ranqueia os 5 títulos com maior valor de estoque por ano de vencimento.
    """

    df_filtrado = df.filter(
        (col("tipo_divida") == "Dívida interna") &
        (col("classe_carteira") == "Mercado") &
        (col("vencimento_ano") >= 2018)
    )

    df_total_anual = df_filtrado.groupBy("vencimento_ano").agg(
        sum("valor_estoque").alias("estoque_total_anual")
    )

    df_com_percentual = df_filtrado.join(df_total_anual, on="vencimento_ano", how="left") \
        .withColumn("percentual_contribuicao", 
                    (col("valor_estoque") / col("estoque_total_anual")) * 100)

    janela_ordenacao = Window.partitionBy("vencimento_ano").orderBy(desc("valor_estoque"))
    df_ranqueado = df_com_percentual.withColumn("rank_ano", row_number().over(janela_ordenacao))

    df_top5_por_ano = df_ranqueado.filter(col("rank_ano") <= 5)

    return df_top5_por_ano
