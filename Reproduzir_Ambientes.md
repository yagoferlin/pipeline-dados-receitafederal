🌐 Passo a Passo para Reproduzir o Pipeline

1️⃣ Pré-requisitos
```
Antes de tudo, certifique-se de que o ambiente possui:

Docker ≥ 24.0

docker-compose ≥ 1.29

Python 3.10+ (para scripts Spark, opcional se usar container)

Portas livres: 8081 (Airflow), 4040 (Spark UI), 9000/9001 (MinIO)

Recursos mínimos: 8GB RAM, 4 cores CPU

Para ambientes em cluster ou nuvem, ajuste memória e núcleos do Spark conforme volume de dados.
```

2️⃣ Clonar o repositório
```
git clone <URL_DO_REPOSITORIO>
cd pipeline-dados
```
3️⃣ Configurar diretórios de dados
```
Crie as pastas para o Data Lake, se ainda não existirem:

mkdir -p data-lake/bronze
mkdir -p data-lake/silver
mkdir -p data-lake/gold
mkdir -p data-lake/gold_tmp
```
4️⃣ Subir containers
```
docker-compose up -d --build

Isso irá iniciar:

Airflow → orquestra os jobs.

Spark → executa os scripts de transformação.

MinIO → armazenamento compatível com S3 para o Data Lake.
```
5️⃣ Verificar interfaces
```
Airflow: http://localhost:8081
 → usuário admin / senha admin123

MinIO: http://localhost:9001
 → usuário minioadmin / senha minioadmin

Spark UI: http://localhost:4040
 → monitoramento de jobs Spark
```
6️⃣ Executar jobs Spark manualmente
```
Dentro do container Spark, execute qualquer script manualmente:

spark-submit-job ingesta_bronze.py

Outros scripts: ingesta_silver.py, controle_atualizacao.py, ingesta_gold.py, contagem_matriz.py.
```
7️⃣ Executar pipeline automático via Airflow
```
Acesse o Airflow (localhost:8081).

Habilite o DAG dag.py.

Clique em Trigger DAG → o pipeline executa automaticamente:
Bronze → Silver → Controle Atualizacao → Gold
```
8️⃣ Verificação e logs
```
Spark: Spark UI (localhost:4040) → monitoramento de tarefas, tempo de execução e erros.

Airflow: Logs de cada task no DAG → detalhes de execução e possíveis falhas.

MinIO: Confirme se arquivos Parquet foram criados nas camadas Bronze, Silver e Gold.
```
9️⃣ Limpando ambiente
```
Para parar containers e liberar recursos:

docker-compose down

Para reiniciar o pipeline, apenas suba novamente os containers.
```
1️⃣0️⃣ Dicas adicionais
```
Em servidor Linux, ajuste volumes de dados e caminhos no docker-compose.yml.

Em nuvem (OCI, AWS, GCP), configure endpoints de MinIO/S3 e redirecione portas ou use load balancer.

Sempre use gold_tmp → garante atomicidade e rollback seguro antes de atualizar a camada Gold.
```
