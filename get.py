import os
import sys
import geopandas as gpd

maindir = "/common/ecap/prospector_data/results/stages/4-added_nearest_protected_area"

for dir in os.listdir(maindir):
    state_dir = os.path.join(maindir, dir, "gpkg")
    print(state_dir)
    for file in os.listdir(state_dir):
        print("FILE:", file)
        if file.endswith(".gpkg"):
            file_path = os.path.join(state_dir, file)

            output_file_gpkg = file_path.replace(".gpkg", "-2.gpkg")

            data = gpd.read_file(file_path)
            gdf = gpd.GeoDataFrame(data)
            print("GDF loaded")
            for col in gdf.columns:
                if "nearest" in col:
                    print(gdf[col][0])
                    gdf[col] = gdf[col] / 1000
                    print(gdf[col][0])
                    print("-" * 10)

            gdf.to_file(output_file_gpkg)
            print("GDF saved again")
