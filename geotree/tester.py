import rustf

print(dir(rustf))
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
    load_geometries_typed_dict,
    load_geometries_reg_dict,
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

regio = "saarland"

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
# secondary_src_data = gpd.read_feather(secondary_file)

# Transform to EPSG:25832 if necessary
primary_src_crs = CRS[regio]

if primary_src_crs != 25832:
    primary_src_data = primary_src_data.set_crs(
        primary_src_crs, allow_override=True
    ).to_crs(25832)


# TEST
PRIMARY_DATA = load_geometries_reg_dict(primary_src_data)

SD = {
    0: [[123, 123], [231, 2342], [533, 245], [485, 321], [212, 432]],
    1: [[253, 113], [931, 242], [133, 445], [335, 321], [152, 832]],
}

# SECONDARY_DATA = load_geometries_reg_dict(secondary_src_data)

# print(type(PRIMARY_DATA[0]))

for i in range(len(PRIMARY_DATA.keys())):
    PriDat_polygon = PRIMARY_DATA[i]

    res = rustf.calculate_shortest_distance(PriDat_polygon, [0, 1], SD)
    break
