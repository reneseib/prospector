import os
import re
import geopandas as gpd
import pandas as pd
from shapely import wkt
from shapely.geometry import Point
from multiprocessing import Pool
from pconfig import config

f = "/common/ecap/prospector_data/results/stages/7-added_nearest_agrargen/baden_wuerttemberg/gpkg/baden_wuerttemberg-7-added_nearest_agrargen.gpkg"

fa = "/common/ecap/prospector_data/results/stages/2-added_centroids/baden_wuerttemberg/gpkg/baden_wuerttemberg-2-added_centroids.gpkg"

regios = config["area"]["subregions"]
stages = [
    # "4-added_nearest_protected_area",
    "5-added_nearest_substation",
    # "6-added_slope",
    # "6_5-added_slope_results",
    # "7-added_nearest_agrargen",
    # "8-added_solar_data",
    # "8_5-added_geportal_data",
]


for regio in regios:

    file = f"/common/ecap/prospector_data/results/final/{regio}/gpkg/{regio}_final.gpkg"

    data = gpd.read_file(file)
    gdf = gpd.GeoDataFrame(data)

    df = pd.DataFrame(gdf)

    dir = f"/common/ecap/prospector_data/results/final/{regio}/gpkg"
    if not os.path.exists(dir):
        os.mkdir(dir)

    df.to_csv(file.replace("gpkg", "csv"))
