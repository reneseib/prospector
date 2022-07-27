bla = [
    "id",
    "area_m2",
    "area_ha",
    "lsg_overlap",
    "nsg_overlap",
    "biosphaere_overlap",
    "fauna_flora_overlap",
    "nationalparks_overlap",
    "naturmonumente_overlap",
    "vogelschutz_overlap",
    "lsg_distance",
    "nsg_distance",
    "biosphaere_distance",
    "fauna_flora_distance",
    "nationalparks_distance",
    "naturmonumente_distance",
    "vogelschutz_distance",
    "nearest_solar",
    "nearest_wind",
    "nearest_hydro",
    "nearest_residential",
    "nearest_commercial",
    "nearest_industrial",
    "platform",
    "nearest_railways",
    "nearest_roads",
    "nearest_substations",
    "nearest_substation_info",
    "np_slope_abs",
    "np_slopes_to_centroid",
    "nearest_agrargen",
    "nearest_agrargen_info",
    "solar_DNI",
    "solar_GHI",
    "solar_DIF",
    "solar_PVOUT_csi",
    "solar_GTI_opta",
    "solar_OPTA",
    "solar_TEMP",
    "soil_score",
    "geometry",
]


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


regios = [
    # "saarland",  # final stage - done
    # "berlin",  # final stage - done
    "bremen",  # final stage - done
    "hamburg",  # final stage - done
    "hessen",  # final stage - done
    "rheinland_pfalz",  # final stage - done
    "sachsen",  # final stage - done
    "schleswig_holstein",  # final stage - done
    "brandenburg",  # done
    "bayern",  # final stage - done
    "nordrhein_westfalen",  # final stage - done
    "thueringen",  # final stage - done
    "niedersachsen",  # final stage - done
    "sachsen_anhalt",  # final stage - done
    "baden_wuerttemberg",  # final stage - done
    "mecklenburg_vorpommern",
]

for regio in regios:

    file = f"/common/ecap/prospector_data/results/final/{regio}/gpkg/{regio}_final.gpkg"

    # if os.path.exists(file):
    #     os.remove(file)

    # data = gpd.read_file(file)
    # gdf = gpd.GeoDataFrame(data)
    #
    # print(gdf.columns)

    # drop_cols = []
    # for col in gdf.columns:
    #     if "np_" in col and not "slope" in col:
    #         drop_cols.append(col)
    #
    # gdf = gdf.drop(columns=drop_cols)
    #
    # df = pd.DataFrame(gdf)
    #
    # dir = f"/common/ecap/prospector_data/results/final/{regio}/csv"
    # if not os.path.exists(dir):
    #     os.mkdir(dir)
    #
    # df.to_csv(file.replace("gpkg", "csv"))
    # print(regio, "saved as csv")
