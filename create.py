import pandas as pd
import random as r
r.seed('7326362')
from classes_bd import *

df_curso = pd.read_csv('./dados/curso.csv')
df_ies = pd.read_csv('./dados/ies.csv')
df_aluno = pd.read_csv('./dados/aluno.csv')
df_aluno['NU_DATA_NASCIMENTO'] = df_aluno['NU_DATA_NASCIMENTO'].astype('datetime64')
route = {'host': 'localhost', 'user': 'root', 'password': 'senha', 'database': 'bd'}

ies = Table(**route, table_name='ies')
curso = Table(**route, table_name='curso')
aluno = Table(**route, table_name='aluno')
ies.createTableMYSQL(df_ies)
ies.addPrimaryKey('CO_IES')
curso.createTableMYSQL(df_curso)
curso.addPrimaryKey('CO_CURSO')
curso.addForeignKey('CO_IES', 'CO_IES', 'ies')
aluno.createTableMYSQL(df_aluno)
aluno.addPrimaryKey('ID_ALUNO', 'CO_IES', 'CO_CURSO')
aluno.addForeignKey('CO_IES', 'CO_IES', 'curso')
aluno.addForeignKey('CO_CURSO', 'CO_CURSO', 'curso')

for i in range(500):
    ies.addRow(df_ies.iloc[i].values)
for i in range(486):
    curso.addRow(df_curso.iloc[i].values)
for i in range(500):
    aluno.addRow(df_aluno.iloc[i].values)

aluno.createTrigger()
aluno.createProcedure()
ies.closeCon()
curso.closeCon()
aluno.closeCon()