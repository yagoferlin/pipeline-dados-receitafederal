from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import timedelta
import pendulum

# ======================== Configurações padrão da DAG ========================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

local_tz = pendulum.timezone("America/Sao_Paulo")

# ======================== DAG ========================
with DAG(
    dag_id="PipelineMensal_ReceitaFederal",
    description="Pipeline Mensal Ingestão Dados Receita Federal",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 2, 6, tz=local_tz),
    schedule_interval="0 5 5 * *",
    catchup=False,
    tags=[
        "mensal", "spark", "bronze", "silver", "gold",
        "receita", "federal", "receita_federal"
    ],
) as dag:

    # ======================== Variável ANOMES ========================
    # Pega a variável do Airflow ou usa a data de execução do DAG
    anomes = Variable.get(
        "ANOMES_RECEITA",
        default_var="{{ execution_date.strftime('%Y-%m') }}"
    )

    # ======================== Tasks ========================
    Bronze = BashOperator(
        task_id="Bronze",
        bash_command=f"""
        docker exec spark spark-submit /opt/spark/jobs/ingesta_bronze.py --anomes {anomes}
        """
    )

    Silver = BashOperator(
        task_id="Silver",
        bash_command="""
        docker exec spark spark-submit /opt/spark/jobs/ingesta_silver.py
        """
    )

    Controle_Atualizacao = BashOperator(
        task_id="Controle_Atualizacao",
        bash_command="""
        docker exec spark spark-submit /opt/spark/jobs/controle_atualizacao.py
        """
    )

    Gold = BashOperator(
        task_id="Gold",
        bash_command="""
        docker exec spark spark-submit /opt/spark/jobs/ingesta_gold.py
        """
    )

    Bronze >> Silver >> Controle_Atualizacao >> Gold