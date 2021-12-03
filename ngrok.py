from flask_ngrok import run_with_ngrok
from flask import Flask, json
from flask import request
from flask import jsonify
import mysql.connector as sql

class conBd():
    def __init__(self, host, user, password, database):
        self.con = sql.connect(host=host, user=user, password=password, database=database)
        self.cursor = self.con.cursor(buffered=True)
        self.database = database
    
    def closeCon(self):
        self.cursor.close()
        self.con.close()

app = Flask(__name__)
run_with_ngrok(app)
route = {'host': 'localhost', 'user': 'root', 'password': 'jPql18ght5z@', 'database': 'bd'}
database_name = route['database']

@app.route('/addrow', methods = ['GET'])
def addRow():
    global route
    table_name = request.args.get('table')
    values = request.args.get('values')
    values = values.split('|')
    connection = conBd(**route)
    s = ''
    for i in values:
        s += f"'{i}', "
    s = s[:-2]
    connection.cursor.execute(f"INSERT INTO {table_name} VALUES ({s})")
    connection.con.commit()
    connection.closeCon()
    return 'Tudo certo'

@app.route('/delrow', methods = ['GET'])
def delRow():
    global route
    table_name = request.args.get('table')
    parameters = request.args.get('parameters')
    parameters = [i[:i.index('=')+1] + f"'{i[i.index('=')+1:]}'" for i in parameters.split('|')]
    connection = conBd(**route)
    s = ''
    for i in parameters:
        s += f"{i} AND "
    s = s[:-5]
    connection.cursor.execute(f"DELETE FROM {table_name} WHERE {s}")
    connection.con.commit()
    connection.closeCon()
    return f"DELETE FROM {table_name} WHERE {s}"

@app.route('/updaterow', methods = ['GET'])
def updateRow():
    global route
    table_name = request.args.get('table')
    new_parameters = request.args.get('new_parameters')
    new_parameters = [i[:i.index('=')+1] + f"'{i[i.index('=')+1:]}'" for i in new_parameters.split('|')]
    comparison_parameters = request.args.get('comparison_parameters')
    comparison_parameters = [i[:i.index('=')+1] + f"'{i[i.index('=')+1:]}'" for i in comparison_parameters.split('|')]
    connection = conBd(**route)
    s_update = ''
    s_comparison = 'WHERE '
    for i in new_parameters:
        s_update += f"{i}, "
    s_update = s_update[:-2]
    for i in comparison_parameters:
        s_comparison += f"{i} AND "
    s_comparison = s_comparison[:-5]
    connection.cursor.execute(f"UPDATE {table_name} SET {s_update} {s_comparison}")
    connection.con.commit()
    connection.closeCon()
    return f"UPDATE {table_name} SET {s_update} {s_comparison}"

@app.route('/readrow', methods = ['GET'])
def readRow():
    global route
    connection = conBd(**route)
    table_name = request.args.get('table')
    columns = request.args.get('columns')
    if columns != None:
        columns = [i for i in columns.split('|')]
    parameters = request.args.get('parameters')
    if parameters != None:
        a = []
        for i in parameters.split('|'):
            for j in i:
                if not j.isidentifier() and not j.isnumeric():
                    index = i.index(j)
            a.append(i[:index+1] + f"'{i[index+1:]}'")
        parameters = a.copy()
        del a
    s_columns = ''
    s_parameters = 'WHERE '
    if columns == None and parameters == None:
        connection.cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{database_name}' AND TABLE_NAME='{table_name}'")
        keys = connection.cursor.fetchall()
        connection.cursor.execute(f"SELECT * FROM {table_name}")
        values = connection.cursor.fetchall()
        connection.closeCon()
        output = [{keys[j][0]: values[i][j] for j in range(len(values[0]))} for i in range(len(values))]
        return jsonify(output)
    elif columns == None:
        for i in parameters:
            s_parameters += f"{i} AND "
        s_parameters = s_parameters[:-5]
        connection.cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{database_name}' AND TABLE_NAME='{table_name}'")
        keys = connection.cursor.fetchall()
        connection.cursor.execute(f"SELECT * FROM {table_name} {s_parameters}")
        values = connection.cursor.fetchall()
        connection.closeCon()
        output = [{keys[j][0]: values[i][j] for j in range(len(values[0]))} for i in range(len(values))]
        return jsonify(output)
        #return jsonify(keys)
    elif parameters == None:
        for i in columns:
            s_columns += f'{i}, '
        s_columns = s_columns[:-2]
        connection.cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{database_name}' AND TABLE_NAME='{table_name}'")
        keys = connection.cursor.fetchall()
        connection.cursor.execute(f"SELECT {s_columns} FROM {table_name}")
        values = connection.cursor.fetchall()
        connection.closeCon()
        output = [{keys[j][0]: values[i][j] for j in range(len(columns))} for i in range(len(values))]
        return jsonify(output)
    else:
        for i in parameters:
            s_parameters += f"{i} AND "
        s_parameters = s_parameters[:-5]
        for i in columns:
            s_columns += f'{i}, '
        s_columns = s_columns[:-2]
        connection.cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{database_name}' AND TABLE_NAME='{table_name}'")
        keys = connection.cursor.fetchall()
        connection.cursor.execute(f"SELECT {s_columns} FROM {table_name} {s_parameters}")
        values = connection.cursor.fetchall()
        connection.closeCon()
        output = [{keys[j][0]: values[i][j] for j in range(len(columns))} for i in range(len(values))]
        return jsonify(output)

@app.route('/select', methods = ['GET'])
def select():
    global route
    connection = conBd(**route)
    connection.cursor.execute(f"SELECT CO_IES, COUNT(*) as QNT_DE_ALUNOS FROM ies NATURAL JOIN aluno GROUP BY CO_IES HAVING COUNT(*) > 100")
    result = connection.cursor.fetchall()
    output = [{'CO_IES': i[0], 'QNT_DE_ALUNOS': i[1]} for i in result]
    return jsonify(output)

@app.route('/procedure', methods = ['GET'])
def procedure():
    global route
    connection = conBd(**route)
    num_ies = request.args.get('CO_IES')
    connection.cursor.execute(f"CALL ver_qtd_aluno({num_ies})")
    result = connection.cursor.fetchall()
    output = [{'CO_IES': i[0], 'QNT_DE_ALUNOS': i[1]} for i in result]
    return jsonify(output)

app.run()