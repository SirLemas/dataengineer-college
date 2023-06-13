# Importando as bibliotecas necessárias para executar no projeto
import csv
import pandas as pd
import psycopg2 as db
import warnings

warnings.filterwarnings('ignore')

# baixando os dados direto no pandas
df_alunos = pd.read_csv('https://dados.al.gov.br/catalogo/datastore/dump/b38d7939-a946-45a8-a46f-640dc38e7fce', sep=',')
df_saude = pd.read_csv('https://dados.al.gov.br/catalogo/dataset/e3a5de4b-423e-45de-a35b-e16f4881415f/resource/87dccaad-76e7-4bbe-827f-b833fd33667e/download/quantidade-de-profissionais-da-saude.csv', sep=';', encoding='latin-1')
df_saude = df_saude.rename(columns={'variável': 'variavel', 'município': 'municipio', 'Quantidade de profissionais da Saúde' : 'qtd_profissionais_saude'})

# criando conexão com o banco
string_connection_db="dbname='dados_brutos' host='localhost' user='postgres' password='p@ssw0rd'"
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

# Confirma as alterações no banco de dados e fecha a conexão com o banco de dados
connection.commit()
cur.close()
connection.close()

# abrindo nova conexão
string_connection_db2="dbname='dados_brutos' host='localhost' user='postgres' password='p@ssw0rd'"
connection2=db.connect(string_connection_db2)
cur2=connection2.cursor()

# faz query para pegar os dados que foram salvos no banco de dados
df_alunos_banco = pd.read_sql("select * from alunos", connection2)
df_saude_banco = pd.read_sql("select * from saude", connection2)

# Limpeza de dados
# removendo dados duplicados
df_alunos_banco.drop_duplicates(keep='first', inplace=True)
df_saude_banco.drop_duplicates(keep='first', inplace=True)

# removendo dados vazios
df_saude_banco.dropna(inplace=True)
df_alunos_banco.dropna(inplace=True)

# removendo colunas que não serão utilizadas
df_alunos_banco.drop(columns=['id', 'variavel','valor'], inplace=True)
df_saude_banco.drop(columns=['id', 'und', 'tags', 'fonte', 'municipio'], inplace=True)

# mudando nome das colunas para não serem confundidas
df_alunos_banco.rename(columns={'nome_municipio':'nome_municipio_aluno'}, inplace=True)
df_saude_banco.rename(columns={'geocodigo': 'codigo_municipio', 'variavel': 'tipo_profissional_saude'}, inplace=True)

# Faz o merge dos data frames
dados_combinados = pd.merge(df_alunos_banco, df_saude_banco, how = 'inner', on = 'codigo_municipio')

print(dados_combinados.columns)
print(dados_combinados.head(n=30))

# Fecha a conexão com o banco de dados
connection2.commit()
cur2.close()
connection2.close()