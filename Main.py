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


def individual_data_general(data_frame):
    print(data_frame.shape)

    dff = pd.DataFrame(data_frame, columns=['INCIDENT_ID', 'OPEN_TIME', 'OWNER_NAME', 'CURRENT_PHASE',
                                            'DESCRIPTION', 'CONTACT_NAME', 'OPENED_BY', 'UPDATED_BY', 'RESOLUTION',
                                            'CATEGORY', 'CLOSE_TIME', 'RESOLUTION_CODE', 'SLA_BREACH', 'UPDATE_ACTION',
                                            'TITLE'])  # en columns se definen los campos que se traen desde la DB
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

        info = {"id": id, "fcreacion": t_creacion, "descripcion": descripcion, "abierto_por": abierto_por,
                "asignado": asignado, "fase": fase, "contacto": contacto, "actualizado_por": actualizado_por,
                "solucion": solucion, "categoria": categoria, "fcerrado": cerrado, "codigo_solucion": codigo_solucion,
                "accion_actualizacion": accion_actualizacion, "sla_cumplido": sla_cumplido, "titulo": titulo}
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
    pdfkit.from_file("temporal.html", "C:/attachmentsSmax/"+id_nombre + ".pdf")


read_query()
# pdfkit.from_file("temporal2.html", "w.pdf")
