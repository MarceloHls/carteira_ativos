from airflow.providers.google.cloud.operators.bigquery import  (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteTableOperator
    

)
from airflow.providers.google.cloud.operators.gcs import  GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
   



from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import sys,time,os,json


sys.path.append('/home/marcelohenriqueleite35/carteira_ativos/jobs')


from datalake.create_datalake import create_datalake
from tb_carteira.create_wrk_carteira import create_carteira
from tb_carteira.add_valor_medio import add_valor_medio
from tb_carteira.add_valor_acoes import add_valor_acao
from tb_carteira.add_currently_value import add_value 
from tb_carteira.add_goal import add_goal
from tb_carteira.add_profity import add_profit

# Cria as variaveis utilizando as chaves/valores do objeto variables

local = os.environ['PWD']

with open(f'{local}/config/schema_tb_carteira.json','r') as file:
    schema_tb_carteira = json.load(file)



project = 'tech-visa-jobs'
dataset = 'carteira_acoes'
location = 'US'
bucket_name='carteira_acoes'
GCP_CONN='gcp_conn'
table_name = 'TB_CARTEIRA'

default_args = {
    'owner': 'Marcelo Henrique',
    'depends_on_past': False,
    # Exemplo: Inicia em 20 de Janeiro de 2021
    'start_date': datetime.now(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # Em caso de erros, tente rodar novamente apenas 0 vez
    'retries': 1,
    # Tente novamente após 30 segundos depois do erro
    'retry_delay': timedelta(seconds=30),
    # Execute uma vez a cada 15 minutos
    'schedule_interval': '*/2 * * * *'
}


with DAG(
    dag_id="carga_tb_carteira",
    default_args=default_args,
) as dag:

    task_create_datalake = PythonOperator(
        task_id='criando_datalake',
        python_callable=create_datalake
    )

    task_create_carteira = PythonOperator(
        task_id='criando_wrk_carteira',
        python_callable=create_carteira
    )
    
    
    task_add_valor_medio = PythonOperator(
        task_id='adicionando_valor_medio_por_acao',
        python_callable=add_valor_medio
    )

    
    task_add_valor_acao = PythonOperator(
        task_id='adicionando_cotacao_acao',
        python_callable=add_valor_acao
    )

    
    task_add_value = PythonOperator(
        task_id='adicionado_valor_atual_investido',
        python_callable=add_value
    )

    
    task_add_goal = PythonOperator(
        task_id='adicionado_meta_participacao_carteira',
        python_callable=add_goal
    )

    task_add_profit = PythonOperator(
        task_id='adicionado_lucro_e_criado_TB_CARTEIRA',
        python_callable=add_profit
    )

    task_create_bucket = GCSCreateBucketOperator(
        task_id='criando_bucket',
        bucket_name=bucket_name,
        storage_class='NEARLINE',
        location=location,
        gcp_conn_id=GCP_CONN,
        project_id=project

    )

    task_send_gcs=LocalFilesystemToGCSOperator(
        task_id='eviando_arquivo_tb_carteira',
        bucket=bucket_name,
        src='/home/marcelohenriqueleite35/carteira_ativos/data/trusted/TB_CARTEIRA.csv',
        dst='TB_CARTEIRA.csv',
        gcp_conn_id=GCP_CONN

    )

    task_create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='criando_dataset',
        dataset_id=dataset,
        project_id=project,
        gcp_conn_id=GCP_CONN

    )


    delete_table = BigQueryDeleteTableOperator(
        task_id='delete_table',
        deletion_dataset_table=f'{project}.{dataset}.{table_name}',
        gcp_conn_id=GCP_CONN,
        ignore_if_missing=True

    )
    task_create_table = BigQueryCreateExternalTableOperator(
        task_id='criando_tabela_bigquery',
        destination_project_dataset_table=f'{project}.{dataset}.{table_name}',
        bucket=bucket_name,
        source_objects=['TB_CARTEIRA.csv'],
        schema_fields=schema_tb_carteira,
        autodetect=True,
        skip_leading_rows=1,
        #  project_id=project,
        gcp_conn_id=GCP_CONN

    )



    task_create_dataset >> delete_table  >> task_create_table
    task_create_bucket >> task_send_gcs 
    task_create_datalake >> task_create_carteira >> task_add_valor_medio >> task_add_valor_acao 
    task_add_valor_acao >> task_add_value >> task_add_goal >> task_add_profit

    task_add_profit >> task_send_gcs >> task_create_table