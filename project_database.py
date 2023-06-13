#modulo timedelta, datetime do pacote datetime
from datetime import timedelta, datetime
import csv
import pandas as pd
import psycopg2 as db
import warnings

#importar o módulo DAG para instanciar um objeto DAG
from airflow import DAG

#Operadores
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

warnings.filterwarnings('ignore')

#criando operadores defaults, dando informações de quem é o proprietário, data de inicio, quantas vezes repetir se der falha e o delay de cada repetição.
default_args = {
    'owner': 'Talles e Felipe',
    'start_date': datetime(2023,3,4),
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

#instanciando a DAG
project_db_dag = DAG('project_db_dag',
    default_args=default_args,
    description='Fazendo o download de bases, salvando no banco de dados e realizando o tratamento delas.',
    schedule_interval='0 0 1 * *',
    catchup=False,
    tags=['db, project', 'dataenginer']
)

#criando funções que serão chamadas pelo PythonOperator
def insert_data_to_database():
    #baixando os dados direto no pandas
    df_alunos = pd.read_csv('https://dados.al.gov.br/catalogo/datastore/dump/b38d7939-a946-45a8-a46f-640dc38e7fce', sep=',')
    df_saude = pd.read_csv('https://dados.al.gov.br/catalogo/dataset/e3a5de4b-423e-45de-a35b-e16f4881415f/resource/87dccaad-76e7-4bbe-827f-b833fd33667e/download/quantidade-de-profissionais-da-saude.csv', sep=';', encoding='latin-1')
    #renomeando algumas colunas para salvar no banco de dados
    df_saude = df_saude.rename(columns={'variável': 'variavel', 'município': 'municipio', 'Quantidade de profissionais da Saúde' : 'qtd_profissionais_saude'})

    # criando conexão com o banco
    string_connection_db="dbname='dados_brutos' host='172.18.0.3' user='airflow' password='airflow'"
    connection=db.connect(string_connection_db)
    cur=connection.cursor()

    # gerando queries de insert e declarando arrays para adicionar os dados
    data_alunos = []
    data_saude = []
    query_alunos = "INSERT INTO alunos (codigo_municipio, nome_municipio, ano, variavel, ensino_rede, ensino_tipo, valor) values (%s, %s, %s, %s, %s, %s, %s)"
    query_saude = "INSERT INTO saude (geocodigo, municipio, variavel, ano, quantidade_profissionais_saude, und, tags, fonte) values (%s, %s, %s, %s, %s, %s, %s, %s)"

    # gerando array de alunos
    for index, row in df_alunos.iterrows():
        data_alunos.append((row.co_mun, row.no_mun, row.ano, row.variavel, row.ensino_rede, row.ensino_tipo, row.valor))

    # gerando array de saude
    for index, row in df_saude.iterrows():
        data_saude.append((row.geocodigo, row.municipio, row.variavel, row.ano, row.qtd_profissionais_saude, row.und, row.tags, row.fonte))

    # criando tuple para adicionar de uma vez no banco de dados
    data_alunos_tuple=tuple(data_alunos)
    data_saude_tuple=tuple(data_saude)

    # executando queries de inserção no banco
    cur.executemany(query_alunos,data_alunos_tuple)
    print('Dados de Alunos inseridos com sucesso!')
    cur.executemany(query_saude,data_saude_tuple)
    print('Dados de Saude inseridos com sucesso!')

    # confirma as alterações no banco de dados
    connection.commit()
    cur.close()
    connection.close()

def get_datas_to_our_database_and_treatment():
    # criando conexão com o banco
    string_connection_db="dbname='dados_brutos' host='172.19.0.3' user='airflow' password='airflow'"
    connection=db.connect(string_connection_db)
    cur=connection.cursor()

    df_alunos_banco = pd.read_sql("select * from alunos", connection)
    df_saude_banco = pd.read_sql("select * from saude", connection)

    # removendo dados duplicados
    df_alunos_banco.drop_duplicates(keep='first', inplace=True)
    df_saude_banco.drop_duplicates(keep='first', inplace=True)

    # removendo dados vazios
    df_saude_banco.dropna(inplace=True)
    df_alunos_banco.dropna(inplace=True)

    # removendo colunas que não serão utilizadas
    df_alunos_banco.drop(columns=['id', 'variavel','valor'], inplace=True)
    df_saude_banco.drop(columns=['id', 'und', 'tags', 'fonte'], inplace=True)

    # mudando nome das colunas para não serem confundidas
    df_alunos_banco.rename(columns={'nome_municipio':'nome_municipio_aluno', 'ano':'ano_matricula_aluno'}, inplace=True)
    df_saude_banco.rename(columns={'geocodigo': 'codigo_municipio', 'municipio': 'nome_municipio_profissional_saude', 'variavel': 'tipo_profissional_saude'}, inplace=True)

    # Faz o merge dos data frames
    dados_combinados = pd.merge(df_alunos_banco, df_saude_banco)
    print(dados_combinados.columns)
    print(dados_combinados.head(n=30))

    cur.close()
    connection.close()

    

#criando primeira tarefa
insert_data_to_database_task = PythonOperator(task_id='insert_data_to_database_task', python_callable=insert_data_to_database, dag=project_db_dag)
#criando segunda tarefa
get_datas_to_our_database_and_treatment_task = PythonOperator(task_id='get_datas_to_our_database_and_treatment_task', python_callable=get_datas_to_our_database_and_treatment, dag=project_db_dag)

#criando ordem de execução das tarefas
insert_data_to_database_task >> get_datas_to_our_database_and_treatment_task