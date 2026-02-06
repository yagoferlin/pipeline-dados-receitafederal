from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import os

############################################################### Spark ###############################################################

spark = (
    SparkSession.builder
    .appName("Ingesta Receita Federal - Silver")
    .enableHiveSupport()
    .getOrCreate()
)

############################################################### JSON ###############################################################

tabelas = ['tb_municipios', 'tb_estabelecimentos']

############################################################### Criando as funções ###############################################################

def log(msg):
    """Função simples para logs detalhados"""
    print(f"[LOG - {datetime.now()}] {msg}")

def processa_lote(file_paths, tabela, silver_path, is_first_lote, repartition_num=100):
    '''
    Processa um lote de arquivos Parquet

    Parameters
    ----------
    file_paths : Caminho
        Caminho dos arquivos na origem
    tabela : Tabela
        Nome da tabela a ser carregada/tratada
    silver_path : Caminho
        Caminho dos arquivos no destino
    is_first_lote : Parâmetro
        Caso seja o primeiro arquivo, overwrite. A partir do segundo na mesma tabela, ele appenda
    repartition_num : Parâmetro
        Número de partições para particionar os dados afim de melhorar a perfomance do script 
    '''
    log(f"Iniciando lote com {len(file_paths)} arquivos")
    
    df = spark.read.parquet(*file_paths)

    # Tratar datas
    datas_cols = [c for c in df.columns if c.startswith("data")]
    for c in datas_cols:
        df = df.withColumn(c, F.to_date(F.col(c), "yyyyMMdd"))

    # Tratamento CNPJs
    if tabela == 'tb_estabelecimentos':
        df = (
            df.withColumn("cnpj_basico", F.regexp_replace(F.col("cnpj_basico"), "\D+", ""))
              .withColumn("cnpj_ordem", F.regexp_replace(F.col("cnpj_ordem"), "\D+", ""))
              .withColumn("cnpj_dv", F.regexp_replace(F.col("cnpj_dv"), "\D+", ""))
              .withColumn("cnpj_completo",
                          F.concat(
                              F.col("cnpj_basico").substr(1,2), F.lit("."),
                              F.col("cnpj_basico").substr(3,3), F.lit("."),
                              F.col("cnpj_basico").substr(6,3), F.lit("/"),
                              F.col("cnpj_ordem"), F.lit("-"),
                              F.col("cnpj_dv")
                          )
              )
        )

        # Situação cadastral
        df = df.withColumn("situacao_cadastral",
                        F.when(F.col("situacao_cadastral") == "01", "NULA")
                         .when(F.col("situacao_cadastral") == "02", "ATIVA")
                         .when(F.col("situacao_cadastral") == "03", "SUSPENSA")
                         .when(F.col("situacao_cadastral") == "04", "INAPTA")
                         .when(F.col("situacao_cadastral") == "05", "ATIVA NAO REGULAR")
                         .when(F.col("situacao_cadastral") == "08", "BAIXADA")
                         .otherwise("DESCONHECIDO")
        )

        # Padroniza CEP e telefones
        for c in ["cep","telefone_1","telefone_2","fax"]:
            if c in df.columns:
                df = df.withColumn(c, F.regexp_replace(c, "[^0-9]", ""))

        # Identificador Matriz/Filial
        df = df.withColumn("identificador_matriz_filial",
                           F.when(F.col("identificador_matriz_filial") == "1", "SIM")
                            .otherwise("NAO")
                            )

    elif tabela == 'tb_municipios':
        if 'idMunicipio' in df.columns:
            df = df.withColumn('idMunicipio', F.col('idMunicipio').cast('Long'))

    # Padroniza strings
    string_cols = [c[0] for c in df.dtypes if c[1] == 'string']
    for c in string_cols:
        df = df.withColumn(c, F.trim(F.upper(F.col(c))))

    # Colunas de controle
    # Lista de colunas para gerar hash (excluindo colunas de controle)
    colunas_hash = [c for c in df.columns if c not in ['sys_datimportacao', 'sys_datatualizacao', 'sys_vlrhash']]

    df = df.withColumn('sys_datimportacao', F.current_timestamp()) \
        .withColumn('sys_datatualizacao', F.lit(None).cast('timestamp')) \
        .withColumn('sys_vlrhash', F.sha2(F.concat_ws("||", *colunas_hash), 256))


    # Reparticiona antes de salvar
    df = df.repartition(repartition_num)

    # Escolhe modo: overwrite se for primeiro lote, append caso contrário
    mode = 'overwrite' if is_first_lote else 'append'
    df.write.mode(mode).parquet(silver_path)
    log(f"Lote gravado com sucesso ({mode}): {silver_path}")

def ingesta_silver(tabela, batch_size=5, repartition_num=100):
    '''
    Processa um lote de arquivos Parquet

    Parameters
    ----------
    tabela : Tabela
        Nome da tabela a ser carregada
    batch_size : Parâmetro
        Número de arquivos a serem processados por cada vez
    repartition_num : Parâmetro
        Número de partições para particionar os dados afim de melhorar a perfomance do script 
    '''
    log(f"Iniciando ingesta da tabela: {tabela}")
    
    bronze_path = f'/data/bronze/{tabela}'
    silver_path = f'/data/silver/{tabela}'

    # Lista arquivos parquet
    all_files = [os.path.join(bronze_path, f) for f in os.listdir(bronze_path) if f.endswith('.parquet')]
    log(f"{len(all_files)} arquivos encontrados no Bronze")

    # Flag para identificar o primeiro lote
    first_lote = True

    # Processa em lotes
    for i in range(0, len(all_files), batch_size):
        lote = all_files[i:i+batch_size]
        processa_lote(lote, tabela, silver_path, is_first_lote=first_lote, repartition_num=repartition_num)
        first_lote = False  # depois do primeiro, todos serão append

############################################################### Chamada das funções ###############################################################

for t in tabelas:
    ingesta_silver(t, batch_size=5, repartition_num=100)
