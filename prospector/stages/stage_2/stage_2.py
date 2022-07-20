import os, sys

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from pyrosm import OSM, get_data
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon, mapping
import numpy as np

# Custom imports
from pconfig import config

for dir in config["init"]["prospector_package_path"]:
    sys.path.append(dir)

from util import util


for dir in config["init"]["proj_path"]:
    if os.path.isdir(dir):
        proj_path = dir

main_dir = os.path.join(proj_path, list(config["directories"].keys())[0])
src_data_dir = os.path.join(main_dir, "src_data")
geo_data_dir = os.path.join(src_data_dir, "geo_data")
results_dir = os.path.join(main_dir, "results")


def f_stage_2(regio, stage="2-added_centroids"):
    """
    Stage 2:
    Drop unnecssary columns and add multiple features for each geometry
    """
    gdf = util.load_prev_stage_to_gdf(regio, stage)

    gdf = gdf.set_crs(config["epsg"][regio], allow_override=True)

    # Drop unnecssary columns forever
    drop_cols = ["landuse", "timestamp", "version", "tags", "osm_type", "changeset"]

    gdf.drop(columns=drop_cols, inplace=True)

    print(f"REGIO: {regio}")
    if len(gdf) > 0:
        # Since we will add features in two different projections (original and 4326), we should copy the GDF at the very beginning, do the calculations for each on separately and eventually merge the columns of the transformed GDF to the original one.

        # 0. Copy gdf to trans_gdf
        trans_gdf = gdf.copy()

        # 1. Transform CRS to 4326 at trans_gdf
        trans_gdf = trans_gdf.to_crs(4326)

        ###### Hereafter we do all calculations on both dataframes ######

        # 2. Add column 'centroid' to both
        gdf["np_centroid"] = gdf["geometry"].centroid.apply(
            lambda x: np.array([x[0] for x in x.coords.xy]).astype(np.float64)
        )

        trans_gdf["np_centroid_4326"] = trans_gdf["geometry"].centroid.apply(
            lambda x: util.geom2arr(x, reverse_coords=True)
        )

        # 2. Add column 'geomarr' - the exterior coords of geometry as np.array
        gdf["np_geomarr"] = gdf["geometry"].apply(lambda x: util.geom2arr(x))

        trans_gdf["np_geomarr_4326"] = trans_gdf["geometry"].apply(
            lambda x: util.geom2arr(x, reverse_coords=True)
        )

        # 3. Add column 'pointcloud' - the dissolved geometry as np.array of single points
        gdf["np_pointcloud"] = gdf["np_geomarr"].apply(lambda x: util.pointcloud(x))

        trans_gdf["np_pointcloud_4326"] = trans_gdf["np_geomarr_4326"].apply(
            lambda x: util.pointcloud(x)
        )

        # 4. Add column 'extrema' - the most Western, Northern, Eastern and Soutern points of the geometry as np.array
        gdf["np_extrema"] = gdf["np_pointcloud"].apply(lambda x: util.get_extrema(x))

        trans_gdf["np_extrema_4326"] = trans_gdf["np_pointcloud_4326"].apply(
            lambda x: util.get_extrema(x)
        )

        # 5. Add column 'bbox' - the bbox of the geometry as np.array
        gdf["np_bbox"] = gdf["np_pointcloud"].apply(lambda x: util.get_bbox(x))

        trans_gdf["np_bbox_4326"] = trans_gdf["np_pointcloud_4326"].apply(
            lambda x: util.get_bbox(x)
        )

        # Merge the trans_gdf data into the regular gdf
        pd_gdf = pd.DataFrame(gdf)
        pd_trans_gdf = pd.DataFrame(trans_gdf)

        trans_cols_to_keep = [
            "np_centroid_4326",
            "np_geomarr_4326",
            "np_pointcloud_4326",
            "np_extrema_4326",
            "np_bbox_4326",
            "id",  # IMPORTANT: Keep id, as merging is based on the same ids
        ]

        mrgd_df = pd.merge(
            pd_gdf,
            pd_trans_gdf[trans_cols_to_keep],
            how="outer",
            on="id",
        )

        gdf = gpd.GeoDataFrame(mrgd_df)

        # Before saving, we need to convert all np.arrays to strings
        for col in gdf.columns:
            if col.startswith("np_"):
                gdf[col] = gdf[col].apply(lambda x: util.stringify(x))

        # Save processing results to disk
        stage_successfully_saved = util.save_current_stage_to_file(gdf, regio, stage)

        if stage_successfully_saved != False:

            print(f"Feature columns for '{regio}' added and GDF saved to file")
            print("")

            return True
        else:
            return False

    else:
        return False


def arr2string(arr):
    if type(arr) == np.ndarray:
        return np.array_repr(arr)

    elif type(arr) == list and type(arr[0]) == np.ndarray:
        return str([np.array_repr(x) for x in arr])
    else:
        return str(arr)
