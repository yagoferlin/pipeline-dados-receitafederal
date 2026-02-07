🚀 Pipeline de Dados com Spark, Airflow e MinIO

Este projeto é um pipeline de dados completo que utiliza Apache Spark, Airflow e MinIO para ingestão, processamento e armazenamento de dados em camadas Bronze → Silver → Gold.

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
⚙️ Tecnologias Utilizadas
```
🐍 Python – Scripts de ingestão e transformação.

⚡ Apache Spark 3.5.0 – Processamento distribuído.

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
spark-submit-job controle_atualizacao.py
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
🤝 Contribuição
```
Faça um fork do repositório.

Crie uma branch para sua feature: git checkout -b feature/nova-feature

Faça commit das alterações: git commit -m "Adiciona nova feature"

Faça push para sua branch: git push origin feature/nova-feature

Abra um Pull Request.
```
📝 Licença
```
Este projeto está sob a MIT License, permitindo uso, cópia, modificação e distribuição, desde que o aviso de copyright seja mantido.
Não há garantias sobre o funcionamento do software.
```
