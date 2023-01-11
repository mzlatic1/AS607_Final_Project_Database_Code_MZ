import psycopg2
import arcpy
import os

### THIS SCRIPT IS A SUPPLEMENT TO THE fc_into_pg SCRIPT WHERE IT RECREATES THE TABLE FOUND IN POSTGRES ###


def recreate_table(connection, cursor, folder_path, fc_name, dbname, schema, wkt_num):
    connection = connection # PG connection
    cursor = cursor # PG cursor to execute SQL statements below

    feature_class = os.path.join(folder_path, fc_name)
    pg_name = fc_name.lower()

    # ArcPy names data types differently than in SQL, the sql_conv_values dictionary
    # will be used to ensure data types are declared appropriately
    sql_conv_values = {
        'Integer': 'INTEGER',
        'SmallInteger': 'INTEGER',
        'Double': 'DOUBLE PRECISION',
        'String': 'VARCHAR',
        'Date': 'TIMESTAMP'
    }
    fields = {}
    for f in arcpy.ListFields(feature_class): # ArcPy function to pull data type information and converts name to SQL equivalent
        if f.type != 'OID' and f.type != 'Geometry':
            fields[f.name] = f.type
    for f in fields:
        fields[f] = sql_conv_values[fields[f]]

    # Lines 32 - 40 creates the SQL expression necessary to create a table in a PG database
    field_names = [f for f in fields.keys()]
    table_string = ''
    index = 0
    while index != len(field_names):
        if index != len(field_names) - 1:
            table_string = table_string + f'{field_names[index]} {fields[field_names[index]]},\n'
        else:
            table_string = table_string + f'{field_names[index]} {fields[field_names[index]]}'
        index += 1

    # Cursor then executes the SQL statements necessary to replace the existing table, if it does exists
    cursor.execute(f"""DROP TABLE IF EXISTS "{dbname}".{schema}.{pg_name}""")
    create_table = f"""create table "{dbname}".{schema}.{pg_name} ({table_string})"""
    cursor.execute(create_table)
    connection.commit()

    # Cursor then finishes by establishing a geometry field, as well as, an objectid field
    shp_type = arcpy.da.Describe(feature_class)['shapeType']
    cursor.execute(f"""\
    select AddGeometryColumn('{dbname}', '{schema}', '{pg_name}', 'geom', {wkt_num}, '{shp_type}', 2);""")
    cursor.execute(f"""\
    alter table "{dbname}".{schema}.{pg_name} add pg_objectid serial primary key""")
    connection.commit()

    # The script returns the newly created PG table in the form of a string for the fc_into_pg script to use
    return f'"{dbname}".{schema}.{pg_name}'
