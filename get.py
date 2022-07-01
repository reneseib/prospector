import os
import sys
import geopandas as gpd

maindir = "/common/ecap/prospector_data/results/stages/3-filtered_by_intersection_protected_area"

for dir in os.listdir(maindir):
    state_dir = os.path.join(maindir, dir, "gpkg")
    print(state_dir)
    for file in os.listdir(state_dir):
        print("FILE:", file)
        if file.endswith(".gpkg"):
            file_path = os.path.join(state_dir, file)
            os.remove(file_path)
