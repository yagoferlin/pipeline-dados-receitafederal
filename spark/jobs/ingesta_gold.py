from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os

############################################################### Spark ############################################################### 

spark = (
    SparkSession.builder
    .appName("Ingesta Gold Incremental")
    .getOrCreate()
)

############################################################### Parâmetros ############################################################### 

tabelas = [
    {"tabela": "tb_municipios", "chaves": ["idMunicipio"]},
    {"tabela": "tb_estabelecimentos", "chaves": ["cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial"]}
]

path_silver = "/data/silver/"
path_gold   = "/data/gold/"
path_tmp    = "/data/gold_tmp/"

col_controle = "txtControleAtualizacao"
col_hash     = "sys_vlrhash"

fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)
Path = spark._jvm.org.apache.hadoop.fs.Path

############################################################### Cria Funções Auxiliares ############################################################### 
def gold_existe_e_nao_vazia(path: str) -> bool:
    p = Path(path)
    if not fs.exists(p):
        return False
    return len(fs.listStatus(p)) > 0

############################################################### Processa tabela para Gold ############################################################### 
for t in tabelas:
    tabela = t["tabela"]
    chaves = t["chaves"]

    print(f"\nProcessando {tabela}")

    silver_path = f"{path_silver}{tabela}/"
    gold_path   = f"{path_gold}{tabela}/"
    tmp_path    = f"{path_tmp}{tabela}/"

    # Le Silver
    df_silver = spark.read.parquet(silver_path)

    # Deduplicação
    window = Window.partitionBy(*chaves).orderBy(F.col(col_hash).desc())
    df_silver = (
        df_silver
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Controle Atualização
    df_novos = df_silver.filter(F.col(col_controle) == "NOVO")
    df_atualizados = df_silver.filter(F.col(col_controle) == "ATUALIZADO")

    qtd_novos = df_novos.count()
    qtd_atualizados = df_atualizados.count()

    print(f"NOVOS: {qtd_novos} | ATUALIZADOS: {qtd_atualizados}")

    # Roda primeira carga (Gold vazia)
    if not gold_existe_e_nao_vazia(gold_path):
        print("Primeira carga da Gold")

        (
            df_silver
            .drop(col_controle)
            .write.mode("overwrite")
            .parquet(tmp_path)
        )

        fs.delete(Path(gold_path), True)
        fs.rename(Path(tmp_path), Path(gold_path))
        continue

    # Le gold
    df_gold = spark.read.parquet(gold_path)

    # Dados atualizados
    if qtd_atualizados > 0:
        cond = [df_gold[c] == df_atualizados[c] for c in chaves]

        df_merge = (
            df_gold.alias("g")
            .join(df_atualizados.alias("s"), cond, "left")
            .select([
                F.when(
                    F.col("s." + chaves[0]).isNotNull(),
                    F.current_timestamp()
                ).otherwise(F.col("g.sys_datatualizacao")).alias("sys_datatualizacao")
                if c == "sys_datatualizacao"
                else F.coalesce(F.col(f"s.{c}"), F.col(f"g.{c}")).alias(c)
                for c in df_gold.columns
            ])
        )
    else:
        df_merge = df_gold

    # Dados novos
    if qtd_novos > 0:
        df_final = df_merge.unionByName(
            df_novos.drop(col_controle)
        )
    else:
        df_final = df_merge

    # Escreve sem perder os dados
    print("Gravando Gold com segurança")

    df_final.write.mode("overwrite").parquet(tmp_path)

    fs.delete(Path(gold_path), True)
    fs.rename(Path(tmp_path), Path(gold_path))

    print(f"{tabela} atualizada com sucesso")

print("\nPROCESSO FINALIZADO COM SUCESSO")
