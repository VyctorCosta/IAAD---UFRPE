import mysql.connector as sql

class conBd():
    def __init__(self, host, user, password, database):
        self.con = sql.connect(host=host, user=user, password=password, database=database)
        self.cursor = self.con.cursor(buffered=True)
        self.database = database
    
    def closeCon(self):
        self.cursor.close()
        self.con.close()

class Table(conBd):
    def __init__(self, host, user, password, database, table_name):
        super().__init__(host, user, password, database)
        self.table_name = table_name

    def createTableMYSQL(self, dataframe):
        s = ''
        for i in range(len(dataframe.columns)):
            if dataframe.dtypes[i] == 'int64':
                df_type = 'INT'
            elif dataframe.dtypes[i] == 'float64':
                df_type = 'FLOAT(3,1)'
            elif dataframe.dtypes[i] == 'object':
                df_type = 'VARCHAR(100)'
            elif dataframe.dtypes[i] == 'bool':
                df_type = 'BOOLEAN'
            elif dataframe.dtypes[i] == 'datetime64[ns]':
                df_type = 'DATE'
            s += f'{dataframe.columns[i]} {df_type}, '
        s = s[:-2]
        self.cursor.execute(f'CREATE TABLE IF NOT EXISTS {self.table_name} ({s})')
    
    def addPrimaryKey(self, *key_name):
        s = ''
        for i in key_name:
            s += f'{i}, '
        s = s[:-2]
        self.cursor.execute(f"SELECT column_key FROM information_schema.columns WHERE table_name = '{self.table_name}' AND table_schema = '{self.database}'")
        list_pk = self.cursor.fetchall()
        check_pk = False
        for i in list_pk:
            if i[0] != '':
                check_pk = True
        if check_pk:
            self.cursor.execute(f'ALTER TABLE {self.table_name} DROP PRIMARY KEY, ADD PRIMARY KEY({s})')
        else:
            self.cursor.execute(f'ALTER TABLE {self.table_name} ADD PRIMARY KEY({s})')
    
    def addForeignKey(self, foreignkey, primarykey, referenceTable):
        self.cursor.execute(f'ALTER TABLE {self.table_name} ADD FOREIGN KEY ({foreignkey}) REFERENCES {referenceTable}({primarykey})')

    def createTrigger(self):
        self.cursor.execute(f"CREATE TRIGGER inc_qtd_matricula AFTER INSERT ON aluno FOR EACH ROW UPDATE curso SET QT_MATRICULA_TOTAL = (QT_MATRICULA_TOTAL + 1) WHERE co_curso = NEW.co_curso;")

    def createProcedure(self):
        self.cursor.execute(f"CREATE PROCEDURE ver_qtd_aluno (codigo_ies INT) SELECT CO_IES, COUNT(*) as QNT_DE_CURSOS FROM ies NATURAL JOIN curso WHERE CO_IES = codigo_ies GROUP BY CO_IES")
    

    #------------------ Processo CRUD ---------------------------
    # parameters = [atributos de cada coluna em ordem separado por virgula] Ex: ['1', 'UNIVERSIDADE FEDERAL DE MATO GROSSO', 'UFMT', 'Pública Federal', 'Universidade', 'Centro-Oeste']
    def addRow(self, parameters):
        s = ''
        for i in parameters:
            s += f"'{str(i)}', "
        s = s[:-2]
        self.cursor.execute(f"INSERT INTO {self.table_name} VALUES ({s})")
        self.con.commit()

    # parameters = {Nome da coluna especifica: valor da coluna especifica} Ex: {'CO_IES': 1, 'REGIAO': 'Sul'}
    def delRow(self, parameters):
        s = ''
        for i in parameters:
            s += f"{i}='{parameters[i]}' AND "
        s = s[:-5]
        self.cursor.execute(f"DELETE FROM {self.table_name} WHERE {s}")
        self.con.commit()

    # columns = [Nome das das colunas que serão retornadas no select] Ex: ['CO_IES', 'NO_IES'] | Parameters = {Nome da coluna especifica: [valor da coluna especifica, tipo da comparação]} Ex: {'CO_IES': ['1', '='], 'NU_DATA_NASCIMENTO': ['1990-01-01', '>=']}
    def readRow(self, columns=[], parameters={}):
        s_columns = ''
        s_parameters = 'WHERE '
        for i in columns:
            s_columns += f'{i}, '
        if s_columns == '':
            s_columns = '*'
        else:
            s_columns = s_columns[:-2]
        for i in parameters:
            s_parameters += f"{i} {parameters[i][1]} '{parameters[i][0]}' AND "
        if len(parameters) == 0:
            s_parameters = ''
        else:
            s_parameters = s_parameters[:-5]
        self.cursor.execute(f"SELECT {s_columns} FROM {self.table_name} {s_parameters}")
        return self.cursor.fetchall()
    
    # new_parameters = {Nome da coluna especifica: valor da coluna especifica} Ex: {'NO_IES': 'Universidade de Brasília', 'SG_IES': 'UNB'} comparison_parameters = {Nome da coluna especifica: valor da coluna especifica} Ex: {'SG_IES': 'UFPE'}
    def updateRow(self, new_parameters, comparison_parameters):
        s_update = ''
        s_old = 'WHERE '
        for i in new_parameters:
            s_update += f"{i} = '{new_parameters[i]}', "
        s_update = s_update[:-2]
        for i in comparison_parameters:
            s_old += f"{i} = '{comparison_parameters[i]}' AND "
        s_old = s_old[:-5]
        self.cursor.execute(f"UPDATE {self.table_name} SET {s_update} {s_old}")
        self.con.commit()