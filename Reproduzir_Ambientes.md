🔧 Pré-requisitos
```
Para rodar o pipeline em outro ambiente, você precisa garantir:

Docker & Docker Compose

Docker ≥ 24.x

Docker Compose (integrado ao Docker Desktop ou separado)
```
Teste:
```
docker --version
docker compose version

Portas livres

Airflow: 8081

Spark UI: 4040

MinIO: 9000 (endpoint) e 9001 (console)

Se alguma porta estiver ocupada, altere em docker-compose.yml.
```
Espaço em disco
```
Para dados brutos da Receita Federal e Parquet intermediário/final, tenha ≥10GB livres.

Ajuste /data-lake para o caminho do ambiente, se necessário.
```
Rede
```
Se estiver em ambiente corporativo, verifique que URLs externas (ex: Receita Federal) podem ser acessadas.
```
🏗 Estrutura do projeto
```
Certifique-se de que a estrutura é idêntica ao que está no seu README:

pipeline-dados/
├── Dockerfile.spark
├── docker-compose.yml
├── airflow/dags/dag.py
├── data-lake/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── gold_tmp/
└── spark/jobs/
    ├── contagem_matriz.py
    ├── controle_atualizacao.py
    ├── ingesta_bronze.py
    ├── ingesta_silver.py
    └── ingesta_gold.py

⚠️ Todos os diretórios em data-lake devem existir antes de rodar o pipeline, pois Spark escreve diretamente neles.
```
🐳 Configuração do Docker e MinIO
```
docker-compose.yml

Contém serviços: spark, airflow, minio.

Ajuste volumes e paths para o seu ambiente:

volumes:
  - ./data-lake:/data
  - ./spark/jobs:/opt/spark/jobs
```

Garanta que environment de MinIO esteja correto:
```
MINIO_ROOT_USER: minioadmin
MINIO_ROOT_PASSWORD: minioadmin
```

Dockerfile.spark
```
Imagem personalizada com PySpark + dependências.

Pode adicionar pacotes extras, ex: requests, python-dateutil:

RUN pip install requests python-dateutil
```
🚀 Rodando o Pipeline
```
1️⃣ Subir os containers

No diretório do projeto:

docker-compose up -d --build

-d → roda em background

--build → força rebuild das imagens Docker

2️⃣ Verificar serviços

Airflow: http://localhost:8081

MinIO Console: http://localhost:9001

Spark UI: http://localhost:4040

3️⃣ Executar jobs manualmente (opcional)

Se quiser testar cada camada:

# Bronze
spark-submit-job ingesta_bronze.py --anomes 2026-01

# Silver
spark-submit-job ingesta_silver.py

# Controle de Atualização
spark-submit-job controle_atualizacao.py

# Gold
spark-submit-job ingesta_gold.py


Observação: spark-submit-job é a função que você criou no projeto para simplificar chamadas Spark dentro do container.
```
4️⃣ Fluxo automático pelo Airflow
```
Abra Airflow → DAG dag.py deve estar ativo.

Trigger manual ou configure schedule:

DAG executa sequencialmente: Bronze → Silver → Controle Atualizacao → Gold

Logs de execução disponíveis em cada task do DAG.
```
💾 Ajustando para outro ambiente
```
Paths

No seu código Spark, caminhos são fixos (/data/bronze/...).

Se o host tiver outra pasta, ajuste volumes e paths no Docker Compose e nos scripts.
```
Variáveis de ambiente
```
TOKEN Receita Federal → altere em ingesta_bronze.py se for diferente.
```
Performance
```
Spark configura:

.config("spark.driver.memory", "2g")
.config("spark.executor.memory", "2g")
.config("spark.sql.shuffle.partitions", "4")

Para máquinas maiores, aumente memória e número de partições.
```
Arquivos grandes
```
Scripts lidam com arquivos em chunks (Bronze) e lotes (Silver).

Para datasets maiores, ajuste:

Bronze → tamanho do chunk de download (1MB por default)

Silver → batch_size e repartition_num
```
📊 Testando o pipeline
```
Verifique se dados aparecem em:

data-lake/bronze
data-lake/silver
data-lake/gold

Confira logs do Spark dentro do container:

docker logs -f <nome_container_spark>

No Airflow, visualize logs de cada task para checar:

Download

Processamento Silver

Controle de atualização

Atualização Gold
```
🔄 Backup e segurança
```
Gold → gravado incremental e seguro usando pasta temporária (gold_tmp).

Silver → backup temporário para evitar sobrescrita incorreta.

Se houver falha, dados originais não são perdidos.
```
