import psycopg2
import datetime
import arcpy
import sys
import os

arcpy.env.workspace = 'in_memory' # Writes all outputs to computers RAM instead of writing to disk

active_layer = 'https://services6.arcgis.com/ubm4tcTYICKBpist/arcgis/rest/services/BCWS_ActiveFires_PublicView/FeatureServer/0'

# Imports active feature layer to local machine for quicker processing
fc_active = arcpy.FeatureClassToFeatureClass_conversion(active_layer, 'in_memory', 'ACTIVE_BCWS_ActiveFires_Points_ORIG')

this_year = datetime.datetime.today().year

# Lines 18 - 24 determines which year to use for subsequent SQL queries based on latest updates
if (datetime.date.today() - datetime.date(this_year, 4, 1)).days > 0:
    year = this_year
elif (datetime.date.today() - datetime.date(this_year, 4, 1)).days == 0:
    print('Active and Historical Fire Feature Layers are currently being updated, the script wont update today.')
    sys.exit(1)
else:
    year = this_year - 1

date_query = f"IGNITION_DATE >= timestamp '{year}-04-01 00:00:00'"

false_active = arcpy.SelectLayerByAttribute_management(fc_active, "NEW_SELECTION", date_query, "INVERT")
arcpy.DeleteRows_management(false_active) # Removing all false records based on date query on line 26

postgres_database = r"C:\Users\marko\Documents\SpatialDBs_DataInterop_Student14.sde" # .sde file that connects to PG database
master_fc = os.path.join(postgres_database, 'gis-spatialdbs.student14.as607_final_project_active_and_historical_fires_bc_can')

# Only selecting records after April in the PG master table to see which records have changed in the subsequent lines
in_memory_master_fc = arcpy.ExportFeatures_conversion(master_fc, 'BC_Active_Fires_CHECK', date_query.lower())

# Active feature layer and master feature class in PG have different fields, lines 38 - 43 removes all fields that are different
fields_active = [f.name.lower() for f in arcpy.ListFields(fc_active)]
fields_current = [f.name for f in arcpy.ListFields(in_memory_master_fc)]
keep_fields = []
for field in fields_active:
    if field in fields_current:
        keep_fields.append(field)

arcpy.DeleteField_management(fc_active, keep_fields, 'KEEP_FIELDS') # Deletes non-congruent fields

# Lines 48 - 54 pulls all of the dates from the PG master table and sorts them in ascending order, thus the last record will be our latest date
current_date = []
with arcpy.da.SearchCursor(in_memory_master_fc, 'ignition_date') as SC:
    for row in SC:
        current_date.append(row[0])
current_date.sort()

# Line 55 selects all features in active feature layer, whos dates are after the latest date found in the master PG table
new_features = arcpy.ExportFeatures_conversion(active_layer, 'new_features_check',
                                               f"IGNITION_DATE > timestamp '{current_date[-1]}'")

# Readjusts the objectid's from new feature selection to be greater than the largest objectid found in the master PG table
row_count = int(arcpy.GetCount_management(master_fc)[0])
with arcpy.da.UpdateCursor(new_features, 'OID@') as UC:
    oid = row_count
    for row in UC:
        row[0] = oid + 1
        UC.updateRow(row)
        oid += 1

# Lines 68 - 74 connects to the Postgres database and establishes a cursor to execute SQL statements
password = 'INSERT PASSWORD'
connection = psycopg2.connect(dbname='gis-spatialdbs',
                              user='student14',
                              password=password,
                              host='jhu430607sdb.cnzmwrn1t8z5.us-east-2.rds.amazonaws.com',
                              port=5432)
cursor = connection.cursor()

# If theres at least one row in the new selection (Line 55), then it will append new results to master PG table
if int(arcpy.GetCount_management(new_features)[0]) > 0:
    arcpy.Append_management(new_features, master_fc, 'NO_TEST')
    # Recalculates spatial index for master PG table
    cursor.execute("""drop index if exists as607_final_project.production_schema.test_live_table_sp_idx;""")
    cursor.execute("""\
    create index test_live_table_sp_idx on as607_final_project.production_schema.test_live_table using gist(geom);""")
    connection.commit() # Sends execution requests to Postgres database

    cursor.close()
    connection.close()
    print(f'Appended {arcpy.GetCount_management(new_features)[0]} new features.')
else:
    print('No new records to append, continuing with Update Cursor.')

# Joining the in memory master PG table with the active feature layer based on unique fire number value
arcpy.JoinField_management(in_memory_master_fc, "fire_number", active_layer, "FIRE_NUMBER", keep_fields)

# Lines 95 - 102 looks for any rows with a null join and deletes them from the in memory master PG table
check_null_val = arcpy.SelectLayerByAttribute_management(in_memory_master_fc, 'NEW_SELECTION', "FIRE_NUMBER_1 IS NULL")

if int(arcpy.GetCount_management(check_null_val)[0]) > 0:
    print('The following Fire Number(s) did not match the production fc: ')
    with arcpy.da.SearchCursor(check_null_val, 'fire_number') as SC:
        for row in SC:
            print(row[0])

arcpy.DeleteRows_management(check_null_val)

# All fields that join have the same naming convention as the master PG table, thus, all joined features will have a '_1'
# at the end of each joined field, lines 107 - 111 ensures all fields are properly extrapolated for update check
fields_check = []
for k in keep_fields[2:-1]:
    fields_check.append(k + '_1')

fields_check = keep_fields[2:-1] + fields_check

# Lines 114 - 139 looks for differences between the same two fields (line 92) and writes the row information as tuples
features_2_update = []
num = 0
with arcpy.da.SearchCursor(in_memory_master_fc, ['fire_number'] + fields_check) as UC:
    for row in UC:
        append_update = tuple([row[0]]) + row[11:]

        if row[1] != row[11]:
            features_2_update.append(append_update)
        elif row[2] != row[12]:
            features_2_update.append(append_update)
        elif row[3] != row[13]:
            features_2_update.append(append_update)
        elif row[4] != row[14]:
            features_2_update.append(append_update)
        elif row[5] != row[15]:
            features_2_update.append(append_update)
        elif row[6] != row[16]:
            features_2_update.append(append_update)
        elif row[7] != row[17]:
            features_2_update.append(append_update)
        elif row[8] != row[18]:
            features_2_update.append(append_update)
        elif row[9] != row[19]:
            features_2_update.append(append_update)
        else:
            num += 1

# Checks to see if there are any rows to update, if not, the script will exit
if len(features_2_update) == 0:
    print('There are no features to update, script exiting...')
    sys.exit()
else:
    print(f'{len(features_2_update)} features will be updated.')

# If there are updates, the rows are then converted into a dictionary where the fire number is the dictionary key
update_dict = dict([(r[0], (r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8])) for r in features_2_update])

# The main part of the script where the update occurs and can be seen in the master PG table after run completion
# Lines 153 - 175 uses the update_dict dictionary and searches each fire number and updates from the Python dictionary
row_update = 0
row_no_update = 0
with arcpy.da.UpdateCursor(master_fc, ['fire_number'] + keep_fields[2:-1]) as UC:
    for row in UC:
        if row[0] in update_dict.keys():
            if row[1] >= datetime.datetime.strptime(f'{year}-04-01 00:00:00', '%Y-%m-%d %H:%M:%S'): # Ensures proper fire number is selected
                fire_num = row[0]

                row[1] = update_dict[fire_num][0]
                row[2] = update_dict[fire_num][1]
                row[3] = update_dict[fire_num][2]
                row[4] = update_dict[fire_num][3]
                row[5] = update_dict[fire_num][4]
                row[6] = update_dict[fire_num][5]
                row[7] = update_dict[fire_num][6]
                row[8] = update_dict[fire_num][7]

                UC.updateRow(row) # Sends update request to master PG table, similar to the connection.commit() function
                row_update += 1
            else:
                row_no_update += 1
        else:
            row_no_update += 1

print(f"A total of {row_update} records have been updated and {row_no_update} records weren't updated.")
