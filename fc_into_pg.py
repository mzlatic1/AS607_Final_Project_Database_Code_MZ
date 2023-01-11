import recreate_table # This is the recreate_table script found in the same folder
import datetime
import psycopg2
import arcpy
import os


def fc_to_pg(gdb_or_folder, fc_name, dbname, user, password, host, port, schema):
    path = gdb_or_folder
    fc = os.path.join(path, fc_name)

    # This is how the PG connection is established
    connection = psycopg2.connect(dbname=dbname,
                                  user=user,
                                  password=password,
                                  host=host,
                                  port=port)
    cursor = connection.cursor() # A cursor is created to execute the subsequent SQL statements

    # Lines 21 - 25 determines the spatial reference to ensure the geometry created matches the input feature class
    prj_sp = arcpy.da.Describe(fc)['spatialReference']
    if prj_sp.type == 'Geographic':
        wkt_num = prj_sp.GCSCode
    else:
        wkt_num = prj_sp.PCSCode

    # Line 28 is the function from the recreate_table script that regenerates the table found in PG
    table_name = recreate_table.recreate_table(connection, cursor, path, fc_name, dbname, schema, wkt_num)

    # Since geometry and objectid is already generated from the table, we dont need these two fields
    field_names = [f.name for f in arcpy.ListFields(fc) if f.type != 'OID' and f.type != 'Geometry']
    total_row_count = arcpy.GetCount_management(fc)[0]
    num = 1
    with arcpy.da.SearchCursor(fc, field_names + ['SHAPE@WKT']) as SC:
        # The SHAPE@WKT field is a shape object of the input feature class containing the well-known text geometry
        for row in SC:
            print(f'Processing {num} out of {total_row_count} rows.')
            wkt = row[-1]

            # Lines 41 - 51 converts list items into a single continuous string that can be interpreted as a SQL statement
            insert_into = ', '.join(f for f in field_names)
            list_values = []
            for r in row[:-1]:
                if type(r) is datetime.datetime or type(r) is str:
                    fix_r = str(r).replace("'", "''")
                    list_values.append(f"'{fix_r}'")
                elif r is None:
                    list_values.append('NULL')
                else:
                    list_values.append(str(r))
            values = ', '.join(v for v in list_values)

            # Line 55 is the final string that the cursor interprets as a SQL statement where each row of the input
            # feature class is appended into the newly created PG table
            query = f"""insert into {table_name}({insert_into}, geom)\
             values ({values}, ST_GeometryFromText('{wkt}', 3005));"""

            cursor.execute(query) # The cursor then executes the command
            num += 1

    cursor.execute(f"""\
    create index as607_{fc_name.lower()}_sp_idx on {table_name} using gist(geom);""") # Spatial index is then generated

    connection.commit() # The connection finalizes the executions by sending the updates to the PG database to process
    cursor.close()
    connection.close()

    return table_name # The name of the PG table is returned as a string
