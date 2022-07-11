import warnings

warnings.simplefilter("ignore", UserWarning)

import timeit
from shapely.geometry import Polygon, MultiPolygon
import numpy as np
import pandas as pd
import geopandas as gpd
import os
import sys
import numba
from numba import njit, prange, typeof, typed, types
from numba.typed import Dict
from math import ceil

# Custom Imports
from util import (
    load_geometries,
    generate_leaves,
    get_bbox,
    get_centroid,
    global_polygon_dissolve,
    grow_tree,
    get_leaf_to_load,
    get_leaf_collection,
    batch_centroids,
    get_distance,
    collect_distances,
    calculate_shortest_distance,
    status_bar,
)

"""
The general idea to improve the minimum distance calculation time is to first check if our point (the point we specified) is inside a 'tree leaf'. Tree leaves are the result of combining the vertices of the global BBOX and a weighted center point, the global centroid of our dataset.

If this is not the case, we do not need to continue the search and therefore we can save the many calculations inside this 'leaf'.

If it is inside the 'tree leaf', we go to that leaf and check if our point is inside the sub-leaves.

We repeat this to the lowest level and only then calculate the distances, determine the minimum value from it and return it.
"""
t0 = timeit.default_timer()

#
#
#
#
#

regio = "nordrhein_westfalen"

t1 = timeit.default_timer()

primary_file = f"/common/ecap/prospector_data/results/stages/3-filtered_by_intersection_protected_area/{regio}/gpkg/{regio}-3-filtered_by_intersection_protected_area.feather"

secondary_file = (
    "/common/ecap/prospector_data/src_data/protected_areas/gpkg/lsg_gesamt_de.feather"
)

CRS = {
    "thueringen": 25833,
    "sachsen": 25833,
    "sachsen_anhalt": 25833,
    "berlin": 25833,
    "brandenburg": 25833,
    "baden_wuerttemberg": 25832,
    "bayern": 25832,
    "bremen": 25832,
    "hamburg": 25832,
    "hessen": 25832,
    "mecklenburg_vorpommern": 25832,
    "niedersachsen": 25832,
    "nordrhein_westfalen": 25832,
    "rheinland_pfalz": 25832,
    "schleswig_holstein": 25832,
    "saarland": 25832,
}

primary_src_data = gpd.read_feather(primary_file)

# Transform to EPSG:25832 if necessary
primary_src_crs = CRS[regio]

if primary_src_crs != 25832:
    primary_src_data = primary_src_data.set_crs(
        primary_src_crs, allow_override=True
    ).to_crs(25832)

# Load data to typed dict
PRIMARY_DATA = load_geometries(primary_src_data)

t1 = timeit.default_timer() - t0
print(f"PRIMARY_DATA LOADED - {t1} sec.")


t0 = timeit.default_timer()

secondary_src_data = gpd.read_feather(secondary_file)
secondary_src_data = secondary_src_data.set_crs(25832, allow_override=True)
SECONDARY_DATA = load_geometries(secondary_src_data)

t1 = timeit.default_timer() - t0
print(f"SECONDARY_DATA LOADED - {t1} sec.")


t0 = timeit.default_timer()
TREE = grow_tree(PRIMARY_DATA, SECONDARY_DATA)
t1 = timeit.default_timer() - t0
print(f"Grew the tree - {t1} sec.")

tcalcstart = timeit.default_timer()
SD_centroids = np.array(batch_centroids(SECONDARY_DATA)).astype(np.float64)

for i in range(len(PRIMARY_DATA.keys())):
    keys = list(PRIMARY_DATA.keys())
    key = keys[i]
    PriDat_polygon = PRIMARY_DATA[key]
    PriDat_centroid = get_centroid(PriDat_polygon)
    #
    # print("LEAVES")
    # print(TREE["primary"]["leaves"])
    # print("")
    # print("CURRENT POLY CENTROID")
    # print(PriDat_centroid)
    # print("MIN/MAX BBOX")
    # print([280374.9680000003, 5235854.618000001, 919678.7967999995, 6091118.6339])

    leaf_to_load = get_leaf_to_load(TREE["primary"]["leaves"], PriDat_centroid)
    if leaf_to_load is not None:
        if len(TREE["primary"]["collections"][leaf_to_load]) == 0:

            leaf_collection = get_leaf_collection(
                TREE["primary"]["leaves"], leaf_to_load, SECONDARY_DATA
            )

            TREE["primary"]["collections"][leaf_to_load] = leaf_collection

    else:
        print("LEAVES")
        print(TREE["primary"]["leaves"])
        print("")
        print("CURRENT POLYGON")
        print(PriDat_polygon)
        print("")
        print("POLY CENTROID")
        print(PriDat_centroid)
        sys.exit()
        continue

    # # Then collect the leaves for the child; here PD
    # if len(TREE["secondary"]["collections"][leaf_to_load]) == 0:
    #     TREE["secondary"]["collections"][leaf_to_load] = get_leaf_collection(
    #         TREE["secondary"]["leaves"], leaf_to_load, PRIMARY_DATA
    #     )

    # Once we have the collections, we check the distances
    # to the centroids of all polygons.
    # 1. Get all polygon ids
    polygon_ids = TREE["primary"]["collections"][leaf_to_load]

    # Create empty 'result 2D array' in which we store
    # the values as [ID, DISTANCE]
    id_distances_arr = np.empty((len(polygon_ids), 2)).astype(np.float64)
    idd_sample_arr = np.array([1, 2.345])

    # Prepare an array with all centroids for all polygons
    # within that leaf
    id_distances_arr = collect_distances(
        id_distances_arr, idd_sample_arr, polygon_ids, SD_centroids, PriDat_centroid
    )

    # 5. Sort the id_distances_arr by ascending distance (column [1])
    sorted_id_distances_arr = id_distances_arr[id_distances_arr[:, 1].argsort()]

    # 6. Return the n-th shortest distances
    n = ceil(len(id_distances_arr) * 0.00625)
    if n < 11:
        if len(id_distances_arr) > 7:
            n = 7
        else:
            n = ceil(len(id_distances_arr) / 2)

    if n > 0:
        nth_sorted_id_distances = sorted_id_distances_arr[:n]

        # Load the real polygon points from the IDs of the shortlist
        closest_sd_ids = np.array([x[0] for x in nth_sorted_id_distances]).astype(
            np.float64
        )

        rd_polygon_ids_on_this_leaf = np.array(
            TREE["primary"]["collections"][leaf_to_load]
        ).astype(np.float64)

        t0 = timeit.default_timer()
        shortest_distance = calculate_shortest_distance(
            PriDat_polygon, closest_sd_ids, SECONDARY_DATA
        )
        t1 = timeit.default_timer() - t0

        show = status_bar(i, len(PRIMARY_DATA))

        # print(
        #     f"Min. Distance: {round(shortest_distance / 1000,2)} km  --  Calculation Time: {t1}. sec"
        # )
    else:
        print(f"\nSkipped: PRIMARY_DATA - key:{key}\n")


t2 = timeit.default_timer() - tcalcstart
print()
print("-" * 10)
print(f"For {len(PRIMARY_DATA) * len(SECONDARY_DATA)} calculations: {round(t2,2)} sec.")
