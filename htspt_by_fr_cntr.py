import fc_into_pg # This is the fb_into_pg script found in the same folder
import arcpy

# Most straightforward approach to connecting to your PG table is by generating a .sde file
master_fc = r'C:\Users\marko\OneDrive\Documents\SpatialDBs_DataInterop_Student14.sde\gis-spatialdbs.student14.as607_final_project_active_and_historical_fires_bc_can'

# Setting your workspace to 'in_memory' means that all outputs are stored in your computers RAM and not on disk
arcpy.env.workspace = 'in_memory'

# Lines 11 - 16 finds all unique values in a certain field then sorts then in ascending order
centers = []
with arcpy.da.SearchCursor(master_fc, 'fire_centre') as SC:
    for row in SC:
        if row[0] not in centers:
            centers.append(row[0])
centers.sort()

# Input information to establish connection with PG database
dbname = 'gis-spatialdbs'
user = 'student14'
password = 'INPUT PASSWORD'
host = 'jhu430607sdb.cnzmwrn1t8z5.us-east-2.rds.amazonaws.com'
port = 5432
schema = 'student14'

index = 0
while index != len(centers):
    c = centers[index] # Selecting only one of the fire centers at a time
    hotspot_fc = f'as607_final_project_htspt_results_4_ftcntr_{c}' # Creating an output name
    # Line 31 subsets the PG data table to only show rows that equals to the fire center we're currently processing
    query = arcpy.Select_analysis(master_fc, f'in_memory_master_fc_subset_4_centre_{c}', f"fire_centre = {c}")
    print(f'Processing fire center {c} with a total row count of {arcpy.GetCount_management(query)[0]}.')
    # Conducts the hotspot analysis
    htspt_results = arcpy.OptimizedHotSpotAnalysis_stats(query, hotspot_fc, "current_size",
                                                         "COUNT_INCIDENTS_WITHIN_FISHNET_POLYGONS")
    print(f'Updating: {hotspot_fc} feature class.')
    # Runs the fc_into_pg script to append the results of the hotspot analysis to the respected PG table
    fc_into_pg.fc_to_pg('in_memory', hotspot_fc, dbname, user, password, host, 5432, schema)
    index += 1
