import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# Inicializar Spark Session
spark = SparkSession.builder.appName("ETL Job AWS Glue").getOrCreate()

# Carregar dados de um CSV armazenado no S3
s3_path = "s3://seu-bucket/vendas.csv"
df = spark.read.csv(s3_path, header=True, sep=';', inferSchema=True)

# Mostrar dados carregados
df.show()

# Organizar por região
df_sorted = df.orderBy("região")
print("Tabela organizada por região:")
df_sorted.show()

# Somar o valor total das vendas por região
valor_por_regiao = df.groupBy("região").agg(spark_sum("valor").alias("valor_total"))
print("\nValor total das vendas por região:")
valor_por_regiao.show()
