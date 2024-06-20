import pandas as pd
import pdfkit
import pyodbc
import sqlalchemy
from jinja2 import Environment, FileSystemLoader


def conexion_bd():
    cnxn = pyodbc.connect(r'Driver=SQL Server;Server=WIN-5K58DSURMPA\SQLEXPRESS;Database=SMCOM;Trusted_Connection=yes;')
    cursor = cnxn.cursor()
    print(cursor.connection.closed)
    query = "SELECT * FROM INCIDENTSM1 where INCIDENT_ID = 'SD10003'"

    cursor.execute(query)
    rows = cursor.fetchall()
    print(query)
    for row in rows:
        print(row)
    cursor.close()


def read_query():
    engine = sqlalchemy.create_engine(
        'mssql+pyodbc://WIN-5K58DSURMPA\SQLEXPRESS/SMCOM?Trusted_Connection=yes&driver=ODBC+Driver+13+for+SQL+Server')
    print("conexion cerrada:  ", engine.connect().closed)
    owner = 'linker'
    fecha_ini = '2014-01-01 00:00:00'
    fecha_fin = '2016-01-01 00:00:00'
    query = "SELECT * FROM INCIDENTSM1 where OWNER_NAME = '{owner}'  and OPEN_TIME between '{fecha_ini}' and '{fecha_fin}'".format(
        owner=owner, fecha_ini=fecha_ini, fecha_fin=fecha_fin)
    # print(query)
    df = pd.read_sql_query(query, engine)
    # print(df)
    individual_data_general(df)


def read_query_histo(id):
    engine2 = sqlalchemy.create_engine(
        'mssql+pyodbc://WIN-5K58DSURMPA\SQLEXPRESS/SMCOM?Trusted_Connection=yes&driver=ODBC+Driver+13+for+SQL+Server')
    print("conexion cerrada:  ", engine2.connect().closed)
    query_histo = "SELECT [DATESTAMP] as fecha_hora,[TYPE] as tipo ,[OPERATOR] as operador,[DESCRIPTION] as descripcion FROM ACTSVCMGTM1 where NUMBER='{id}'".format(
        id=id)
    df_histo = pd.read_sql_query(query_histo, engine2)
    df_hys = pd.DataFrame(df_histo, columns=['fecha_hora', 'tipo', 'operador', 'descripcion'])

    array = ["<tr><td >FECHA</td><td >TIPO</td><td>OPERADOR</td><td>DESCRIPCION</td></tr>"]
    for ind_hys in range(len(df_hys)):
        fecha = str(df_hys['fecha_hora'][ind_hys])
        tipo = str(df_hys['tipo'][ind_hys])
        operador = str(df_hys['operador'][ind_hys])
        descripcion = str(df_hys['descripcion'][ind_hys])
        array.append(
            "<tr><td>" + fecha + "</td> <td>" + tipo + "</td><td>" + operador + "</td><td>" + descripcion + "</td></tr>")
    return str(array).replace("[", "").replace("\'", "").replace(",", "").replace("]", "")


def individual_data_general(data_frame):
    print(data_frame.shape)
    # en columns se definen los campos que se traen desde la DB,
    dff = pd.DataFrame(data_frame, columns=['INCIDENT_ID', 'OPEN_TIME', 'OWNER_NAME', 'CURRENT_PHASE',
                                            'DESCRIPTION', 'CONTACT_NAME', 'OPENED_BY', 'UPDATED_BY', 'RESOLUTION',
                                            'CATEGORY', 'CLOSE_TIME', 'RESOLUTION_CODE', 'SLA_BREACH', 'UPDATE_ACTION',
                                            'TITLE'])
    # drop_columns = row.dropna()
    for ind in dff.index:
        print(dff['INCIDENT_ID'][ind], dff['OPEN_TIME'][ind], dff['DESCRIPTION'][ind])

        id = str(dff['INCIDENT_ID'][ind])
        t_creacion = str(dff['OPEN_TIME'][ind])
        descripcion = str(dff['DESCRIPTION'][ind])
        asignado = str(dff['OWNER_NAME'][ind])
        fase = str(dff['CURRENT_PHASE'][ind])
        contacto = str(dff['CONTACT_NAME'][ind])
        abierto_por = str(dff['OPENED_BY'][ind])
        actualizado_por = str(dff['UPDATED_BY'][ind])
        solucion = str(dff['RESOLUTION'][ind])
        categoria = str(dff['CATEGORY'][ind])
        cerrado = str(dff['CLOSE_TIME'][ind])
        codigo_solucion = str(dff['RESOLUTION_CODE'][ind])
        accion_actualizacion = str(dff['UPDATE_ACTION'][ind])
        sla_cumplido = str(dff['SLA_BREACH'][ind])
        titulo = str(dff['TITLE'][ind])
        historico = ("<table border=\"1\"  style=\"width: 100%; border-collapse: collapse;\">" + read_query_histo(id) + "</table>")
        # print(historico)
        info = {"id": id, "fcreacion": t_creacion, "descripcion": descripcion, "abierto_por": abierto_por,
                "asignado": asignado, "fase": fase, "contacto": contacto, "actualizado_por": actualizado_por,
                "solucion": solucion, "categoria": categoria, "fcerrado": cerrado, "codigo_solucion": codigo_solucion,
                "accion_actualizacion": accion_actualizacion, "sla_cumplido": sla_cumplido, "titulo": titulo,
                "historico": historico}
        id_para_name = id
        html_form(info, id_para_name)
        print("===========================================")


def html_form(infor, id_nombre):
    environment = Environment(loader=FileSystemLoader("templates/"))
    template = environment.get_template("plantilla1.html")
    info = infor

    contenido = template.render(info)

    file = open("temporal.html", "w")
    file.write(contenido)
    file.close()
    pdfkit.from_file("temporal.html", "C:/attachmentsSmax/" + id_nombre + ".pdf")


read_query()
# pdfkit.from_file("temporal2.html", "w.pdf")

# engine = sqlalchemy.create_engine('mssql://usuario:ABc12.@18.219.249.186\SQLEXPRESS/SMCOM?driver=SQL Server')
# print("conexion cerrada:  ", engine.connect().closed)
