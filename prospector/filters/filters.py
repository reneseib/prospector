import sys

# append the path with parent directories
parent_dirs = ["validate", "filters", "util", "converter"]
for dir in parent_dirs:
    sys.path.append(f"/Users/shellsquid/dev/osm/prospector/{dir}")
# ================================================================= #

import os
import geopandas as gpd

# CUSTOM IMPORTS
from prospector import prospector
import validate
import util
import converter


def filter_shp_by_size(instance, stateabbr, input_crs, a_range):
    """
    This script filters plots by size, either just "greater than" or
    within a range of sizes.

    Input parameters:
    - State abbreviation: "BB", "SN", etc.
    - File CRS as Int: 4326
    - Area range in hectares as type Tuple: (min_val) or (min_val, max_val)

    Dependcies:
    - Unified state shapefile for the respective state
    """

    src_file_name = stateabbr + "_unified.shp"
    output_file_name = stateabbr + "_target_parcels.gpkg"

    files_dir = os.path.join(instance.proj_path, instance.geodata_dir, stateabbr)

    input_file = os.path.join(files_dir, src_file_name)
    output_file = os.path.join(files_dir, output_file_name)

    file_size = os.path.getsize(input_file) / (1024 * 2024)

    def input_check(stateabbr, input_crs, a_range):
        state_check = validate.valid_state(stateabbr)
        crs_check = validate.valid_crs(input_crs)
        range_check = validate.valid_range(a_range)

        if state_check and crs_check and range_check:
            return True
        else:
            return False

    if input_check(stateabbr, input_crs, a_range) == True:
        # HERE COMES THE TRUE PROCESSING
        print(f"Processing file:\t{src_file_name}")
        print(f"File size:\t\t{round(file_size, 2)} MB")

        # LOAD THE FILE TO GEO DF
        print(f"Start loading file to geodataframe")
        data = gpd.read_file(input_file, crs=input_crs)

        # SET RESULT VAR TO NONE & OVERWRITE WHEN SUCCESSFULLY FILTERED
        target_parcels = None

        # GET AREA KEY FROM CLASS DICT
        area_key = prospector.Prospector.STATE_AREA_KEYS[stateabbr]

        if len(a_range) == 1:
            a_min = a_range[0]
            target_parcels = data[data[area_key] > float(a_min)]

        elif len(a_range) == 2:
            a_min = a_range[0]
            a_max = a_range[1]

            target_parcels = data[
                (data[area_key] > float(a_min)) & (data[area_key] < float(a_max))
            ]

        if target_parcels is not None:
            try:
                print("Filtered file. Now writing to disk...")
                target_parcels.to_file(output_file)
                print("-- END --")
            except Exception as e:
                print("Error while saving data to disk!")
                print(e)
                print("\nProcesses canceled, no file created. Please check your input.")
                pass

    return None
