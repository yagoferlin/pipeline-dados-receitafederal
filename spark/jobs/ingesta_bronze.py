import argparse
import requests
import zipfile
import os
import shutil
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
import time

############################################################### Parâmetros ###############################################################
# Parâmetro para buscar ANOMES passado ao chama o JOB
parser = argparse.ArgumentParser(description="Ingestão Bronze Receita Federal")
parser.add_argument(
    "--anomes",
    required=False,
    help="Ano-mês de referência no formato YYYY-MM (opcional)"
)
args = parser.parse_args()

# Caso não seja passado, passar o ANOMES atual
DATA_REF = datetime.strptime(args.anomes, "%Y-%m") if args.anomes else datetime.now()

# Parâmetros para acessar a URL
TOKEN = "YggdBLfdninEJX9"
BASE_URL = "https://arquivos.receitafederal.gov.br/public.php/webdav"

# Parâmetros para execução
RAW_PATH = "/tmp/raw_receita"
MAX_TENTATIVAS = 3
TIMEOUT = 60
os.makedirs(RAW_PATH, exist_ok=True)

############################################################### Spark ###############################################################

spark = (
    SparkSession.builder
    .appName("Ingesta Receita Federal - Bronze")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

log4j = spark._jvm.org.apache.log4j
logger = log4j.LogManager.getLogger("INGESTA_RECEITA")

############################################################### JSON ###############################################################
# JSON para deixar o script mais dinâmico
arquivo_configs = {
    "Municipios": {
        "tabela": "tb_municipios",
        "columns": ["idMunicipio", "txtMunicipio"]
    },
    "Estabelecimentos": {
        "tabela": "tb_estabelecimentos",
        "columns": [
            "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial",
            "nome_fantasia", "situacao_cadastral", "data_situacao_cadastral",
            "motivo_situacao_cadastral", "nome_cidade_exterior", "codigo_pais",
            "data_inicio_atividade", "cnae_fiscal_principal", "cnae_fiscal_secundaria",
            "tipo_logradouro", "logradouro", "numero", "complemento", "bairro",
            "cep", "uf", "idMunicipio", "ddd_1", "telefone_1", "ddd_2",
            "telefone_2", "ddd_fax", "fax", "email", "situacao_especial", "data_situacao_especial"
        ]
    }
}

overwrite_flags = {key: True for key in arquivo_configs.keys()}

############################################################### Criando as funções ###############################################################

def listar_zips(data_ref):
    '''
    Função que lista os arquivos .zip

    Parameters
    ----------
    data_ref : ANOMES
        ANOMES que será utilizado como referência para buscar os dados
    Returns
    -------
        Lista dos arquivos encontrados
    '''
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        anomes = data_ref.strftime("%Y-%m")
        urls = []

        logger.info(f"[Listagem] Tentativa {tentativa} | Buscando arquivos para {anomes}")

        for prefixo in arquivo_configs.keys():
            # Prefixo criado para puxar arquivos sem numeração
            nomes_teste = [f"{prefixo}.zip"]

            # Prefixo criado para puxar arquivos com numeração (do 0 até o último idenfiticado)
            i = 0
            while True:
                nome = f"{prefixo}{i}.zip"
                url = f"{BASE_URL}/{anomes}/{nome}"
                try:
                    resp = requests.head(url, auth=(TOKEN, TOKEN), timeout=TIMEOUT)
                    if resp.status_code == 200:
                        urls.append(url)
                        logger.info(f"[Listagem] Encontrado: {url}")
                        i += 1 
                    else:
                        break
                except Exception as e:
                    logger.error(f"[Listagem] Erro ao acessar {url}: {e}")
                    break

            # Também testa o arquivo sem número
            url_sem_num = f"{BASE_URL}/{anomes}/{prefixo}.zip"
            try:
                resp = requests.head(url_sem_num, auth=(TOKEN, TOKEN), timeout=TIMEOUT)
                if resp.status_code == 200 and url_sem_num not in urls:
                    urls.append(url_sem_num)
                    logger.info(f"[Listagem] Encontrado: {url_sem_num}")
            except Exception as e:
                logger.error(f"[Listagem] Erro ao acessar {url_sem_num}: {e}")

            if not any(url.endswith(prefixo) or prefixo in url for url in urls):
                logger.info(f"[Listagem] Nenhum arquivo {prefixo} encontrado para {anomes}")

        if urls:
            logger.info(f"[Listagem] Total de {len(urls)} arquivos ZIP encontrados para {anomes}")
            return anomes, urls

        data_ref -= relativedelta(months=1)
        logger.info(f"[Listagem] Nenhum ZIP encontrado para {anomes}, retrocedendo um mês")

    raise Exception("Nenhum ZIP encontrado em todas as tentativas")

def processar_zip(url, modo, columns, bronze_path):
    '''
    Função que faz o download, extração e gravação dos arquivos em parquet

    Parameters
    ----------
    url : Link
        URL onde os arquivos são encontrados
    modo : Modo de escrita
        Caso seja o primeiro arquivo, overwrite. A partir do segundo na mesma tabela, ele appenda
    columns : Colunas
        Headers dos arquivos
    bronze_path : Caminho
        Caminho onde os dados serão gravados
    '''
    nome_zip = url.split("/")[-1]
    zip_path = f"{RAW_PATH}/{nome_zip}"

    logger.info(f"[Download] Iniciando download de {nome_zip}")
    start_time = time.time()

    try:
        with requests.get(url, auth=(TOKEN, TOKEN), stream=True, timeout=TIMEOUT) as resp:
            resp.raise_for_status()
            total_bytes = 0
            prox_log = 100 * 1024 * 1024
            with open(zip_path, "wb") as f:
                for chunk in resp.iter_content(1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        total_bytes += len(chunk)
                        if total_bytes >= prox_log:
                            elapsed = time.time() - start_time
                            logger.info(f"[Download] {nome_zip}: {total_bytes/1024/1024:.2f} MB baixados | Tempo: {elapsed:.1f}s")
                            prox_log += 100 * 1024 * 1024

        elapsed_total = time.time() - start_time
        logger.info(f"[Download] Concluído {nome_zip}, tamanho total {total_bytes/1024/1024:.2f} MB | Tempo total: {elapsed_total:.1f}s")
    except Exception as e:
        logger.error(f"[Download] Erro ao baixar {nome_zip}: {e}")
        return

    try:
        with zipfile.ZipFile(zip_path) as z:
            csv_name = z.namelist()[0]
            csv_path = f"{RAW_PATH}/{csv_name}"
            logger.info(f"[Extração] Extraindo CSV {csv_name}")
            with z.open(csv_name) as origem, open(csv_path, "wb") as out:
                for chunk in iter(lambda: origem.read(1024 * 1024), b""):
                    out.write(chunk)
        logger.info(f"[Extração] Concluído CSV {csv_name}")
    except Exception as e:
        logger.error(f"[Extração] Erro ao extrair {zip_path}: {e}")
        return

    try:
        df = (
            spark.read
            .option("sep", ";")
            .option("encoding", "ISO-8859-1")
            .option("header", "false")
            .csv(csv_path)
            .toDF(*columns)
        )
        logger.info(f"[Spark] CSV lido com {df.count()} linhas, gravando Bronze ({modo})")
        df.write.mode(modo).parquet(bronze_path)
        logger.info(f"[Spark] Gravado com sucesso {csv_name} em {bronze_path}")
    except Exception as e:
        logger.error(f"[Spark] Erro ao processar {csv_path}: {e}")

    # Limpa os arquivos
    try:
        os.remove(csv_path)
        os.remove(zip_path)
        logger.info(f"[Limpeza] Arquivos temporários removidos: {csv_name}, {nome_zip}")
    except Exception as e:
        logger.error(f"[Limpeza] Erro ao remover arquivos temporários: {e}")

############################################################### Chamada das funções ###############################################################

logger.info(f"Iniciando ingestão | DATA_REF={DATA_REF.strftime('%Y-%m')}")
_, zip_urls = listar_zips(DATA_REF)

for url in zip_urls:
    nome_arquivo = url.split("/")[-1]
    tipo_encontrado = next((p for p in arquivo_configs.keys() if nome_arquivo.startswith(p)), None)

    if not tipo_encontrado:
        logger.warning(f"[Skip] Arquivo {nome_arquivo} não corresponde a nenhum tipo conhecido")
        continue

    config = arquivo_configs[tipo_encontrado]
    tabela_destino = config["tabela"]
    columns = config["columns"]
    modo = "overwrite" if overwrite_flags[tipo_encontrado] else "append"
    overwrite_flags[tipo_encontrado] = False

    bronze_path_final = f"/data/bronze/{tabela_destino}"
    processar_zip(url, modo, columns, bronze_path_final)

logger.info("Ingestão finalizada")

spark.stop()
shutil.rmtree(RAW_PATH, ignore_errors=True)