import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)
warnings.simplefilter(action="ignore", category=UserWarning)

import os
import sys
import geopandas as gpd
import pandas as pd

from multiprocessing import Pool

from pconfig import config

src_dir = "/common/ecap/prospector_data/results/stages"
target_dir = "/common/ecap/prospector_data/results/final"

stages = list(config["directories"]["prospector_data"]["results"]["stages"].keys())

stages = [
    # "4-added_nearest_protected_area",
    "5-added_nearest_substation",
    # "6-added_slope",
    "6_5-added_slope_results",
    "7-added_nearest_agrargen",
    "8-added_solar_data",
    "8_5-added_geportal_data",
]


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
    "tags",
]


regios = list(config["area"]["subregions"])

all_columns = []

if __name__ == "__main__":

    # with Pool(processes=6) as pool:

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
                    final_file = os.path.join(
                        target_dir, regio, "gpkg", f"{regio}_final.gpkg"
                    )
                    if os.path.exists(final_file):
                        final_data = gpd.read_file(final_file)
                        final_gdf = gpd.GeoDataFrame(final_data)

                        final_df = pd.DataFrame(final_gdf)
                        gdf_df = pd.DataFrame(gdf)

                        gdf = gdf.drop_duplicates(subset=["area_m2"])

                        gdf_keep_cols = list(
                            gdf_df.columns.difference(final_df.columns)
                        )

                        # Remove duplicate columns
                        for col in gdf_keep_cols:
                            if (
                                "_left" in col
                                or "_right" in col
                                or "_x" in col
                                or "_y" in col
                            ):
                                gdf_keep_cols.remove(col)

                        gdf_keep_cols.append("id")

                        mrgd = pd.merge(
                            final_gdf, gdf[gdf_keep_cols], on="id", how="left"
                        )

                        gdf = gpd.GeoDataFrame(mrgd)

                        # Drop unnecssary & duplicate columns
                        for col in gdf.columns:
                            if (
                                "_left" in col
                                or "_right" in col
                                or "_x" in col
                                or "_y" in col
                            ):
                                gdf = gdf.drop(columns=[col])

                        # print(gdf.columns)

                        gdf.to_file(final_file, driver="GPKG")

                    else:
                        gdf.to_file(final_file, driver="GPKG")
                    print(f">> {regio} {stage}: FINAL FILE CREATED")

                    # os._exit(0)


r = [
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
    "points_ele",
    "tags",
    "solar_DIF",
    "solar_DNI",
    "solar_GHI",
    "solar_GTI_opta",
    "solar_OPTA",
    "solar_PVOUT_csi",
    "solar_TEMP",
    "geometry",
    "soil_score",
]
