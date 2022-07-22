import os, sys


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Stage 0 Imports
import geopandas as gpd
import numpy as np
import timeit
from ast import literal_eval

# Custom imports
from pconfig import config

for dir in config["init"]["prospector_package_path"]:
    sys.path.append(dir)

from util import util
from proxpy.proxpy import ProxyRequest


for dir in config["init"]["proj_path"]:
    if os.path.isdir(dir):
        proj_path = dir
        sys.path.append(dir)

main_dir = os.path.join(proj_path, list(config["directories"].keys())[0])
src_data_dir = os.path.join(main_dir, "src_data")
geo_data_dir = os.path.join(src_data_dir, "geo_data")
results_dir = os.path.join(main_dir, "results")


def f_stage_6_5(regio, stage="6_5-added_slope_results"):
    """
    Stage 6.5:
    Calculate the actual slopes
    """
    print(regio)

    # Load previous stage data
    gdf = util.load_prev_stage_to_gdf(regio, stage)

    if len(gdf) > 0:
        gdf["np_points_ele"] = gdf["points_ele"].apply(lambda x: x.split(", "))

        gdf["np_slopes_to_centroid"] = None
        gdf["np_slope_abs"] = None

        for i in range(len(gdf["np_points_ele"])):
            ele_points = gdf.loc[i, "np_points_ele"]
            if not "Unavailable" in ele_points:
                ele_points = [int(x) for x in ele_points]

                _ele_extrema = ele_points[:4]
                ele_left = _ele_extrema[0]
                ele_top = _ele_extrema[1]
                ele_right = _ele_extrema[2]
                ele_bottom = _ele_extrema[3]

                ele_centroid = ele_points[4]

                _coords_extrema = gdf.loc[i, "np_extrema"]
                coords_left = _coords_extrema[0]
                coords_top = _coords_extrema[1]
                coords_right = _coords_extrema[2]
                coords_bottom = _coords_extrema[3]

                coords_centroid = gdf.loc[i, "np_centroid"]

                # L E F T
                slope_left = 0
                # Height difference
                hd = ele_centroid - ele_left
                if hd != 0:
                    # Distance
                    d = util.get_distance(coords_left, coords_centroid)

                    slope_left = round(hd / d * 100, 1)

                # R I G H T
                slope_right = 0
                # Height difference
                hd = ele_right - ele_centroid
                if hd != 0:
                    # Distance
                    d = util.get_distance(coords_right, coords_centroid)

                    slope_right = round(hd / d * 100, 1)

                #  T O P
                slope_top = 0
                # Height difference
                hd = ele_centroid - ele_top
                if hd != 0:
                    # Distance
                    d = util.get_distance(coords_top, coords_centroid)

                    slope_top = round(hd / d * 100, 1)

                #  B O T T O M
                slope_bottom = 0
                # Height difference
                hd = ele_bottom - ele_centroid
                if hd != 0:
                    # Distance
                    d = util.get_distance(coords_bottom, coords_centroid)

                    slope_bottom = round(hd / d * 100, 1)

                slope = np.array(
                    [[slope_left, slope_right], [slope_top, slope_bottom]]
                ).astype(np.float16)

                # ALTERNATIVE
                # Take the absolute height difference as a sum; the lower, the flatter
                slope_alt = np.array(
                    [
                        abs(slope_left)
                        + abs(slope_right)
                        + abs(slope_top)
                        + abs(slope_bottom)
                    ]
                ).astype(np.float16)

                gdf.at[i, "np_slopes_to_centroid"] = slope
                gdf.at[i, "np_slope_abs"] = slope_alt

        # Drop object-containing and temporary columns
        gdf = gdf.drop(columns=["np_points_ele"])

        print(gdf.loc[10, "np_slopes_to_centroid"])
        print(gdf.loc[10, "np_slope_abs"])
        # Save the final result
        final_save = util.save_current_stage_to_file(gdf, regio, stage)
        print(f"{regio}: all elevations saved")

        if final_save == True:
            return True
        else:
            return False
