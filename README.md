🚀 Pipeline de Dados com Spark, Airflow e MinIO

Este projeto é um pipeline de dados completo que utiliza Apache Spark, Airflow e MinIO para ingestão, processamento e armazenamento de dados em camadas Bronze → Silver → Controle Atualizacao → Gold.

📂 Estrutura do Projeto
```
pipeline-dados/
├── Dockerfile.spark            # Imagem Spark personalizada
├── docker-compose.yml          # Orquestração de containers
├── airflow/
│   └── dags/dag.py             # DAG principal do Airflow
├── data-lake/
│   ├── bronze/                 # Dados crus
│   ├── silver/                 # Dados intermediários
│   ├── gold/                   # Dados finais
│   └── gold_tmp/               # Área temporária para Gold
└── spark/jobs/                 # Scripts Python de ingestão e transformação
    ├── contagem_matriz.py
    ├── controle_atualizacao.py
    ├── ingesta_bronze.py
    ├── ingesta_silver.py
    └── ingesta_gold.py
```

🏗 Camadas de Dados
```
Camada	Descrição
Bronze	Dados brutos, direto da fonte.
Silver	Dados transformados e padronizados.
Gold	Dados finais prontos para análise e relatórios.
```

📊 Desafios e como lidei com eles
```
Por que usamos Parquet

Formato colunar → lê só as colunas necessárias, economizando memória e I/O.

Mantém o schema dos dados → evita erros e garante consistência.

Compatível com Spark e MinIO/S3, com suporte a compressão → mais rápido e eficiente.
```
```
Por que criamos gold_tmp

Área temporária para evitar perda de dados e garantir atomicidade.

Só substituímos o Gold final depois que tudo é processado corretamente.

Permite rollback fácil em caso de falhas e mantém consistência.
```
```
1️⃣ Bronze – Dados Brutos

Baixa arquivos da Receita Federal (.zip) e extrai CSVs.

Possível passar via variables do AirFlow o ANOMES a ser puxado. Se não passar nenhum, ele puxa o ANOMES atual.

Processa downloads em chunks, para lidar com arquivos grandes.

Grava em Parquet na camada Bronze:

Primeiro arquivo → overwrite

Demais arquivos → append

Limpa arquivos temporários após ingestão.
```
```
2️⃣ Silver – Dados Padronizados

Lê arquivos do Bronze em lotes, garantindo eficiência.

Padroniza dados: datas, CNPJs, CEPs, telefones, strings.

Adiciona colunas de controle:

sys_datimportacao (timestamp de ingestão)

sys_datatualizacao (para controle futuro)

sys_vlrhash (hash para identificar mudanças)

Grava em Parquet na camada Silver, usando overwrite ou append conforme o lote.
```
```
3️⃣ Controle de Atualização

Compara Silver com Gold para identificar registros:

NOVO → não existe na Gold

ATUALIZADO → hash mudou

TRATADO_ANTERIORMENTE → hash igual

Atualiza Silver com o status em coluna txtControleAtualizacao.

Faz backup temporário (*_tmp) para garantir consistência antes de substituir dados existentes.
```
```
4️⃣ Gold – Dados Consolidados

Deduplicação usando chaves primárias e hash.

Atualiza incrementalmente:

Novos registros → adicionados

Atualizados → substituem registros existentes, mantendo histórico e timestamps

Grava de forma segura usando pasta temporária, evitando perda de dados em caso de falhas.
```
⚙️ Tecnologias Utilizadas
```
🐍 Python – Scripts de ingestão e transformação.

⚡Apache Spark 3.5.0 – Processamento distribuído.

🕸 Apache Airflow 2.8.1 – Orquestração de workflows.

🏗 MinIO – Armazenamento compatível com S3.

🐳 Docker & Docker Compose – Containerização e execução isolada.
```
🏃‍♂️ Como Rodar o Projeto
```
Clonar o repositório:

git clone <URL_DO_REPOSITORIO>
cd pipeline-dados

Subir os containers:

docker-compose up -d --build
```

Acessar as interfaces:
```
🌐 Airflow: http://localhost:8081

Usuário: admin | Senha: admin123

🗄 MinIO Console: http://localhost:9001

Usuário: minioadmin | Senha: minioadmin

📊 Spark UI: http://localhost:4040
```
Executar jobs Spark manualmente (criei uma função para simplificar a chamada):
```
spark-submit-job {script_a_ser_chamado}.py
```

Fluxo automático pelo Airflow:
```
O DAG dag.py orquestra a execução sequencial dos jobs: Bronze → Silver → Controle Atualizacao → Gold.
```
⚠️ Observações
```
Todos os diretórios de dados (bronze, silver, gold, gold_tmp) devem existir antes da execução.

O pipeline pode ser executado manualmente via Spark ou automaticamente via Airflow.

Certifique-se de que as portas 8081, 4040, 9000 e 9001 estejam livres.
```
📝 Licença
```
Este projeto está sob a MIT License, permitindo uso, cópia, modificação e distribuição, desde que o aviso de copyright seja mantido.
Não há garantias sobre o funcionamento do software.
```
