import asyncio
import aiohttp
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from google.cloud import bigquery
import os




async def fetch_data(session: aiohttp.ClientSession, url: str, payload: dict, delay: int):
    try:
        async with session.post(url, json=payload) as response:
            response.raise_for_status()  # Lança uma exceção para erros HTTP
            data = await response.json()
            await asyncio.sleep(delay)  # Simula um atraso opcional
            return data
    except aiohttp.ClientError as e:
        # Lidar com erros de cliente, como problemas de rede
        return {'error': f'ClientError: {str(e)}'}
    except aiohttp.http_exceptions.HttpProcessingError as e:
        # Lidar com erros específicos do processamento HTTP
        return {'error': f'HttpProcessingError: {str(e)}'}
    except Exception as e:
        # Lidar com outros tipos de exceções
        return {'error': f'Exception: {str(e)}'}

# Função para a Tarefa 1: Requisições Iniciais
def extract_data(**kwargs):
    async def run_task():
        url = 'https://veiculos.fipe.org.br/api/veiculos/ConsultarAnoModeloPeloCodigoFipe'
        df = pd.read_csv('./include/csv/modelos_fipe2.csv', encoding='utf-8')
        payloads = [
            {
                "codigoTipoVeiculo": 1,
                "codigoTabelaReferencia": 312,
                "modeloCodigoExterno": row['Código FIPE']
            } for _, row in df.iterrows()
        ]
        
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_data(session, url, payload, 1) for payload in payloads]
            response_data = await asyncio.gather(*tasks)

            extractions = []
            for item_list, payload in zip(response_data, payloads):
                for item in item_list:
                    value = item.get('Value', '')
                    model_year, fuel_code = value.split('-')
                    
                    extractions.append({
                        "model_year": model_year,
                        "fuel_code": fuel_code,
                        "fipe_code": payload['modeloCodigoExterno']
                    })
        
        # Usar XCom para passar os resultados para a próxima tarefa
        ti = kwargs['ti']
        ti.xcom_push(key='initial_results', value=extractions)
    
    asyncio.run(run_task())


def process_data(**kwargs):
    ti = kwargs['ti']
    initial_results = ti.xcom_pull(task_ids='fetch_initial_data', key='initial_results')

    if not initial_results:
        raise AirflowException("No initial results found")

    async def run_task():
        
        url = 'https://veiculos.fipe.org.br/api/veiculos/ConsultarValorComTodosParametros'
        payloads = [
            {
                "codigoTabelaReferencia": 312,
                "codigoTipoVeiculo": 1,
                "anoModelo": item.get('model_year', ''),
                "codigoTipoCombustivel": item.get('fuel_code', ''),
                "tipoVeiculo": "car",
                "modeloCodigoExterno": item.get('fipe_code', ''),
                "tipoConsulta": "codigo"
            } for item in initial_results
        ]
        
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_data(session, url, payload, 1) for payload in payloads]
            final_results = await asyncio.gather(*tasks)

        
        ti = kwargs['ti']
        ti.xcom_push(key='final_results', value=final_results)

    asyncio.run(run_task())


def save_to_bigquery(**kwargs):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = './include/service_account.json'

    ti = kwargs['ti']
    final_results = ti.xcom_pull(task_ids='fetch_final_data', key='final_results')

    if not final_results:
        raise AirflowException("No final results found")

    # Converter os resultados finais para DataFrame
    df_final_results = pd.DataFrame(final_results)

    # Configurar BigQuery
    project_id = ''
    dataset_id = ''
    table_id = 'fipe_tabela'
    table_name = f'{project_id}.{dataset_id}.{table_id}'

    # Inicializar o cliente BigQuery
    client = bigquery.Client()

    # Configurar o job de carregamento
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Substitui os dados existentes
        autodetect=True,  # Detecta automaticamente o esquema a partir dos dados
    )

    # Carregar os dados para o BigQuery
    load_job = client.load_table_from_dataframe(
        df_final_results, table_name, job_config=job_config
    )
    
    # Esperar o job completar
    load_job.result()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG('data_processing_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    t1 = PythonOperator(
        task_id='fetch_initial_data',
        python_callable=extract_data,
        provide_context=True
    )
    
    t2 = PythonOperator(
        task_id='fetch_final_data',
        python_callable=process_data,
        provide_context=True
    )
    
    t3 = PythonOperator(
        task_id='save_to_bigquery',
        python_callable=save_to_bigquery,
        provide_context=True
    )

    t1 >> t2 >> t3