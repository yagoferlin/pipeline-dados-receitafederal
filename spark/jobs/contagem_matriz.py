from pyspark.sql import SparkSession
from pyspark.sql import functions as F

############################################################### Spark ###############################################################

spark = (
    SparkSession.builder
    .appName("Contagem")
    .enableHiveSupport()
    .getOrCreate()
)

df_est = spark.read.parquet('/data/gold/tb_estabelecimentos')

df_mun = (
        spark.read.parquet(f"/data/gold/tb_municipios/")
        .filter(F.col("txtMunicipio") == "SAO PAULO")
        .select("idMunicipio"))

df = (df_est.join(df_mun, "idMunicipio", "inner").filter(F.col("situacao_cadastral") == "ATIVA"))

df_contagem = df.groupBy("identificador_matriz_filial").agg(F.count("*").alias("quantidade"))

df_contagem.show()