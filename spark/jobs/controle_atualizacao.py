import logging
import shutil
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from functools import reduce

############################################################### Spark ############################################################### 
spark = (
    SparkSession.builder
    .appName("Controle Atualizacao Silver")
    .enableHiveSupport()
    .getOrCreate()
)

############################################################### Log ############################################################### 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

############################################################### Parâmetros ############################################################### 

tabelas = [
    {"tabela": "tb_municipios", "chaves": ["idMunicipio"]},
    {"tabela": "tb_estabelecimentos", "chaves": ["cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial"]}
]

col_hash = "sys_vlrhash"
col_controle = "txtControleAtualizacao"

############################################################### Processando Contorle Atualização ############################################################### 

for t in tabelas:
    tabela = t["tabela"]
    chaves = t["chaves"]

    logger.info(f"Processando tabela {tabela}")

    silver_path = f"/data/silver/{tabela}"
    gold_path = f"/data/gold/{tabela}" 
    temp_path = f"/data/silver/{tabela}_tmp"

    # Le Silver
    try:
        df_silver = spark.read.parquet(silver_path)
        logger.info(f"Silver {tabela} carregada")
    except Exception:
        logger.warning(f"Silver {tabela} não encontrada. Pulando tabela.")
        continue

    # Le Gold
    try:
        df_gold = spark.read.parquet(gold_path)
        logger.info(f"Gold {tabela} carregada")
    except Exception:
        logger.warning(f"Gold {tabela} não encontrada. Todos registros serão NOVO.")
        df_final = df_silver.withColumn(col_controle, F.lit("NOVO"))
        df_final.write.mode("overwrite").parquet(temp_path)
        if os.path.exists(silver_path):
            shutil.rmtree(silver_path)
        shutil.move(temp_path, silver_path)
        continue

    
    # Join Silver x Gold pelas chaves
    join_cond = reduce(lambda a, b: a & b, [df_silver[k] == df_gold[k] for k in chaves])
    df_joined = df_silver.join(df_gold, join_cond, how="left")

    # Se não encontrou na Gold → NOVO
    # Se encontrou e hash diferente → ATUALIZADO
    # Se encontrou e hash igual → TRATADO_ANTERIORMENTE
    # Considera que a primeira chave sendo nula significa que não encontrou
    df_final = df_joined.withColumn(
        col_controle,
        F.when(reduce(lambda a, b: a & b, [df_gold[k].isNull() for k in chaves]), F.lit("NOVO"))
        .when(df_silver[col_hash] != df_gold[col_hash], F.lit("ATUALIZADO"))
        .otherwise(F.lit("TRATADO_ANTERIORMENTE"))
    ).select(df_silver["*"], col_controle)

    # Gravar no caminho temporário
    df_final.write.mode("overwrite").parquet(temp_path)
    logger.info(f"Dados escritos temporariamente em {temp_path}")

    # Substituir Silver
    if os.path.exists(silver_path):
        shutil.rmtree(silver_path)
        logger.info(f"Dados antigos da Silver {tabela} apagados")
    shutil.move(temp_path, silver_path)
    logger.info(f"Dados atualizados com sucesso em {silver_path}")

logger.info("Processo finalizado")
spark.stop()