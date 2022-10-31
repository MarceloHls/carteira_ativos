# python -m venv env
# source env/bin/activate
export LOCAL=`pwd`
export AIRFLOW_HOME=$LOCAL/airflow
export PYTHONPATH=/home/marcelohenriqueleite35/carteira_ativos/jobs
echo $AIRFLOW_HOME
airflow standalone
