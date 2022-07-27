import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)
warnings.simplefilter(action="ignore", category=UserWarning)
import sys
import os
import geopandas as gpd
import pandas as pd

from pconfig import config


"""
Final conversion of columns to make them
filterable when being loaded as gpkg
"""
dir = "/common/ecap/prospector_data/results/final"

regios = list(config["area"]["subregions"])

new_column_names = {
    "id": "id",
    "area_m2": "area_sqm",
    "area_ha": "area_ha",
    "biosphaere_overlap": "overlaps_biosphaere",
    "lsg_overlap": "overlaps_lsg",
    "nsg_overlap": "overlaps_nsg",
    "fauna_flora_overlap": "overlaps_fauna_flora",
    "nationalparks_overlap": "overlaps_nationalpark",
    "naturmonumente_overlap": "overlaps_naturmonument",
    "vogelschutz_overlap": "overlaps_vogelschutz",
    "lsg_distance": "nearest_lsg",
    "nsg_distance": "nearest_nsg",
    "biosphaere_distance": "nearest_biosphaere",
    "fauna_flora_distance": "nearest_fauna_flora",
    "nationalparks_distance": "nearest_nationalpark",
    "naturmonumente_distance": "nearest_naturmonument",
    "vogelschutz_distance": "nearest_vogelschutz",
    "nearest_solar": "nearest_solar",
    "nearest_wind": "nearest_wind",
    "nearest_hydro": "nearest_hydro",
    "nearest_residential": "nearest_residential",
    "nearest_commercial": "nearest_commercial",
    "nearest_industrial": "nearest_industrial",
    "nearest_railways": "nearest_railways",
    "nearest_roads": "nearest_roads",
    "nearest_substations": "nearest_substations",
    "nearest_substation_info": "nearest_substation_info",
    "np_slope_abs": "slope_abs",
    "np_slopes_to_centroid": "slopes_to_centroid",
    "nearest_agrargen": "nearest_agrargen",
    "nearest_agrargen_info": "nearest_agrargen_info",
    "solar_DNI": "solar_DNI",
    "solar_GHI": "solar_GHI",
    "solar_DIF": "solar_DIF",
    "solar_PVOUT_csi": "solar_PVOUT_csi",
    "solar_GTI_opta": "solar_GTI_opta",
    "solar_OPTA": "solar_OPTA",
    "solar_TEMP": "solar_TEMP",
    "soil_score": "soil_score",
    "geometry": "geometry",
}

col_types = {
    "id": int,
    "area_sqm": float,
    "area_ha": float,
    "overlaps_biosphaere": bool,
    "overlaps_lsg": bool,
    "overlaps_nsg": bool,
    "overlaps_fauna_flora": bool,
    "overlaps_nationalpark": bool,
    "overlaps_naturmonument": bool,
    "overlaps_vogelschutz": bool,
    "nearest_lsg": float,
    "nearest_nsg": float,
    "nearest_biosphaere": float,
    "nearest_fauna_flora": float,
    "nearest_nationalpark": float,
    "nearest_naturmonument": float,
    "nearest_vogelschutz": float,
    "nearest_solar": float,
    "nearest_wind": float,
    "nearest_hydro": float,
    "nearest_residential": float,
    "nearest_commercial": float,
    "nearest_industrial": float,
    "nearest_railways": float,
    "nearest_roads": float,
    "nearest_substations": float,
    "nearest_substation_info": str,
    "slope_abs": float,
    "slopes_to_centroid": str,
    "nearest_agrargen": float,
    "nearest_agrargen_info": str,
    "solar_DNI": float,
    "solar_GHI": float,
    "solar_DIF": float,
    "solar_PVOUT_csi": float,
    "solar_GTI_opta": float,
    "solar_OPTA": int,
    "solar_TEMP": float,
    "soil_score": float,
    "geometry": "XXX",  # skip this column
}


def to_type(col, x):
    if col != "geometry":
        if x is not None and x != "None":
            if "[" in str(x):
                x = x.replace("[", "").replace("]", "")

            if col_types[col] == int:
                x = int(x)
            if col_types[col] == float:
                x = float(x)
            if col_types[col] == str:
                x = str(x)
            return x
        else:
            return None
    else:
        return x


regios = [
    # "saarland",
    # "berlin",
    # "bremen",
    # "hamburg",
    # "hessen",
    # "rheinland_pfalz",
    # "sachsen",
    # "schleswig_holstein",
    # "brandenburg",
    # "bayern",
    # "nordrhein_westfalen",
    # "thueringen",
    # "niedersachsen",
    # "sachsen_anhalt",
    # "baden_wuerttemberg",
    "mecklenburg_vorpommern",
]


for regio in regios:
    print(regio)
    file = f"/common/ecap/prospector_data/results/final/{regio}/gpkg/{regio}_final.gpkg"

    data = gpd.read_file(file)

    gdf = gpd.GeoDataFrame(data)

    drop_cols = [
        "basin",
        "farmyard",
        "abandoned",
        "tram",
        "platform",
        "retail",
        "switch",
        "generator",
        "military",
    ]

    for col in gdf.columns:
        if col in drop_cols:
            gdf = gdf.drop(columns=[col])

    old_cols = gdf.columns
    # Iterate over old columns to add new col names
    for col in old_cols:
        new_name = new_column_names[col]
        gdf[new_name] = gdf[col]
        if new_name != col:
            gdf = gdf.drop(columns=[col])

    # # Iterate over new columns to set new types
    for col in gdf.columns:
        gdf[col] = gdf[col].apply(lambda x: to_type(col, x))

    gdf.to_file(file.replace("_final", "_final2"))

    df = pd.DataFrame(gdf)

    df.to_csv(file.replace("gpkg", "csv").replace("_final", "_final2"))

    print("All converted and saved")
