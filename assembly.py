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

keep_columns = {
    "5-added_nearest_substation": [],
    "6_5-added_slope_results": [
        "id",
        "np_slope_abs",
        "np_slopes_to_centroid",
    ],
    "7-added_nearest_agrargen": [
        "id",
        "nearest_agrargen",
        "nearest_agrargen_info",
    ],
    "8-added_solar_data": [
        "id",
        "solar_DNI",
        "solar_GHI",
        "solar_DIF",
        "solar_PVOUT_csi",
        "solar_GTI_opta",
        "solar_OPTA",
        "solar_TEMP",
    ],
    "8_5-added_geportal_data": ["id", "soil_score"],
}


stages = [
    # "4-added_nearest_protected_area",
    "5-added_nearest_substation",
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

    regios = [
        # "saarland",  # final stage - done
        # "berlin",  # final stage - done
        # "bremen",  # final stage - done
        # "hamburg",  # final stage - done
        # "hessen",  # final stage - done
        # "rheinland_pfalz",  # final stage - done
        # "sachsen",  # final stage - done
        # "schleswig_holstein",  # final stage - done
        # "brandenburg",  # done
        # "bayern",  # final stage - done
        # "nordrhein_westfalen",  # final stage - done
        # "thueringen",  # final stage - done
        # "niedersachsen",  # final stage - done
        # "sachsen_anhalt",  # final stage - done
        # "baden_wuerttemberg",  # final stage - done
        "mecklenburg_vorpommern",
    ]
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

                    gdf = gdf.drop_duplicates(subset=["area_m2"])

                    # Load existing final file is it exists.
                    # If not, create new final file
                    final_file = os.path.join(
                        target_dir, regio, "gpkg", f"{regio}_final.gpkg"
                    )
                    print("file exists")
                    print(os.path.exists(final_file))

                    if os.path.exists(final_file):

                        final_data = gpd.read_file(final_file)
                        final_gdf = gpd.GeoDataFrame(final_data)

                        final_df = pd.DataFrame(final_gdf)
                        gdf_df = pd.DataFrame(gdf)
                        print("HERE")
                        print(final_df.columns)
                        print(gdf_df.columns)

                        # # Remove duplicate columns
                        # for col in gdf_keep_cols:
                        #     if (
                        #         "_left" in col
                        #         or "_right" in col
                        #         or "_x" in col
                        #         or "_y" in col
                        #     ):
                        #         gdf_keep_cols.remove(col)

                        mrgd = pd.merge(
                            final_gdf,
                            gdf_df[keep_columns[stage]],
                            on="id",
                            how="left",
                        )

                        print("AFTER MERGE")
                        print(mrgd.columns)

                        gdf = gpd.GeoDataFrame(mrgd)

                        # Drop unnecssary & duplicate columns
                        for col in gdf.columns:
                            if (
                                "_left" in col
                                or "_right" in col
                                or "_x" in col
                                or "_y" in col
                                or col.startswith("np_")
                                and not "slope" in col
                            ):
                                gdf = gdf.drop(columns=[col])
                        print("AFTER MERGE")
                        print(gdf.columns)

                        gdf.to_file(final_file, driver="GPKG")

                    else:

                        print("STARTING FRESH")
                        gdf.to_file(final_file, driver="GPKG")
                    print(f">> {regio} {stage}: FINAL FILE CREATED")


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
