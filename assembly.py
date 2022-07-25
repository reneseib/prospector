import os
import sys
import geopandas as gpd

from pconfig import config

src_dir = "/common/ecap/prospector_data/results/stages"
target_dir = "/common/ecap/prospector_data/results/final"

stages = list(config["directories"]["prospector_data"]["results"]["stages"].keys())

# stages = [
#     "4-added_nearest_protected_area",
#     "5-added_nearest_substation",
#     "6-added_slope",
#     "6_5-added_slope_results",
#     "7-added_nearest_agrargen",
#     "8-added_solar_data",
#     "8_5-added_geportal_data",
# ]

rel_cols = {
    "stage_4": [],
    "stage_5": [
        "nearest_solar",
        "nearest_wind",
        "nearest_hydro",
        "nearest_residential",
        "nearest_commercial",
        "nearest_industrial",
        "nearest_railways",
        "nearest_roads",
    ],
    "stage_6_5": [
        "np_slopes_to_centroid",
        "np_slope_abs",
    ],
    "stage_7": ["nearest_agrargen"],  # TODO!!!!!!!!!!!!
    "stage_8": [
        "solar_PVOUT_csi",
        "solar_DNI",
        "solar_GHI",
        "solar_DIF",
        "solar_GTI_opta",
        "solar_OPTA",
        "solar_TEMP",
    ],
    "stage_8_5": ["soil_score"],
}


drop_cols = [
    "basin",
    "farmland",
    "landuse",
    "meadow",
    "timestamp",
    "version",
    "osm_type",
    "changeset",
    "military",
    "residential",
]


regios = list(config["area"]["subregions"])

all_columns = []

if __name__ == "__main__":
    for stage in stages:
        stage_dir = os.path.join(src_dir, stage)

        for regio in regios:
            regio_dir = os.path.join(stage_dir, regio)

            gpkg_regio_dir = os.path.join(regio_dir, "gpkg")

            for file in os.listdir(gpkg_regio_dir):
                if file.endswith(".gpkg"):
                    file_path = os.path.join(gpkg_regio_dir, file)
                    data = gpd.read_file(file_path)
                    gdf = gpd.GeoDataFrame(data)

                    # Drop all unnecssary columns from gdf
                    cols_to_drop = []
                    for col in gdf.columns:
                        if col in drop_cols:
                            cols_to_drop.append(col)
                    gdf = gdf.drop(columns=cols_to_drop)

                    # Load existing final file is it exists.
                    # If not, create new final file
                    final_file = os.path.join(target_dir, regio, f"{regio}_final.gpkg")
                    if os.path.exists(final_file):
                        final_data = gpd.read_file(final_file)
                        final_gdf = gpd.GeoDataFrame(final_data)
                    else:
                        gdf.to_file(final_file, driver="GPKG")
