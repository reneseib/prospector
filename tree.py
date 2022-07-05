import warnings

warnings.simplefilter("ignore", UserWarning)

import timeit
import random
from shapely.geometry import Point, Polygon, MultiPolygon, mapping, MultiPoint, box
from shapely import wkt
import numpy as np
import geopandas as gpd
import os
import sys
from math import radians, cos, sin, asin, sqrt
from numba import njit, prange
import numba


"""
The general idea to improve computation time for the minimum distance
is to first check if our point of origin (the point we provide) is within
a tree branch/page.

If it is not, we don't need to follow up searching within it and therefore
save the many computation within this page.

if it is within the tree branch/page, we go to that branch and test if our
point is within the sub-branches/sub-pages.

Repeat until lowest level and only then calculate the distances,
get the minimum distance and return it.
"""


# Setup

geo_file = "/common/ecap/prospector_data/results/stages/3-filtered_by_intersection_protected_area/sachsen/gpkg/sachsen-3-filtered_by_intersection_protected_area.gpkg"

lsg_file = (
    "/common/ecap/prospector_data/src_data/protected_areas/gpkg/lsg_gesamt_de.gpkg"
)


# ALL GEO DATA
geo_data = gpd.read_file(geo_file)
geo_gdf = gpd.GeoDataFrame(geo_data).set_crs(25832, allow_override=True).to_crs(4326)

gdf_centroid = geo_gdf["geometry"].centroid
gdf_centroid = gdf_centroid.map(lambda x: np.array(x.xy))

gdf_centroid = gdf_centroid.map(lambda x: np.array([x[0], x[1]], dtype=np.float64))

centroid_arr = np.zeros((len(gdf_centroid), 2), dtype=np.float64)


for i in prange(len(centroid_arr)):
    centroid_arr[i] = gdf_centroid[i].reshape(
        2,
    )


print("GEO DATA LOADED")
# LSG DATA

lsg_data = gpd.read_file(lsg_file)
lsg_gdf = gpd.GeoDataFrame(lsg_data).set_crs(25832, allow_override=True).to_crs(4326)

print("LSG DATA LOADED")

# 1.0  Get total bounds of lsg_gdf, returns np.array of minx, miny, maxx, maxy
global_bounds = np.array(lsg_gdf["geometry"].total_bounds)
global_minx, global_miny, global_maxx, global_maxy = global_bounds


# 2.0 Get centroid of whole lsg_gdf
# 2.1 Get centroids of of all geometries as Points
lsg_centroids = lsg_gdf["geometry"].apply(lambda x: Point(x.centroid))

# 2.2 Make a separate GDF out of the centroid series
centroid_gdf = gpd.GeoDataFrame(lsg_centroids)

# 2.3 Retrieve the centroid coordinates as np.array from the
gdf_centroid_point = MultiPoint(centroid_gdf.geometry).centroid.xy
lsg_centroid = np.array([gdf_centroid_point[0][0], gdf_centroid_point[1][0]])
lsg_centroid_x = lsg_centroid[0]
lsg_centroid_y = lsg_centroid[1]


# 3.0 Make the top level 4 boxes/pages of the tree
#     The boxes all start or end with the global centroid

# Since the syntax for boxes are minx, miny, maxx, maxy the 'left bottom' and
# 'right top' can be retrieved from the existing coordinates.
# Left top and right bottom have to be calculated since the coordinates
# can not be taken from the existing ones. Best we make a function for this:


def make_4_boxes(
    global_minx,
    global_miny,
    global_maxx,
    global_maxy,
    lsg_centroid_x,
    lsg_centroid_y,
):
    #
    # Since the syntax for boxes are minx, miny, maxx, maxy the 'left bottom'
    # and 'right top' can be retrieved from the existing coordinates.
    # Left top and right bottom have to be put together since the coordinates
    # can not be taken from the existing ones.
    #

    p_left_bottom = np.array(
        [global_minx, global_miny, lsg_centroid_x, lsg_centroid_y], dtype=np.float64
    )

    p_right_top = np.array(
        [lsg_centroid_x, lsg_centroid_y, global_maxx, global_maxy], dtype=np.float64
    )

    # Put the coordinates together for 'left top'
    p_left_top_minx = global_minx
    p_left_top_miny = lsg_centroid_y
    p_left_top_maxx = lsg_centroid_x
    p_left_top_maxy = global_maxy

    p_left_top = np.array(
        [p_left_top_minx, p_left_top_miny, p_left_top_maxx, p_left_top_maxy],
        dtype=np.float64,
    )

    # Put the coordinates together for 'right bottom'
    p_right_bottom_minx = lsg_centroid_x
    p_right_bottom_miny = global_miny
    p_right_bottom_maxx = global_maxx
    p_right_bottom_maxy = lsg_centroid_y

    p_right_bottom = np.array(
        [
            p_right_bottom_minx,
            p_right_bottom_miny,
            p_right_bottom_maxx,
            p_right_bottom_maxy,
        ],
        dtype=np.float64,
    )

    pages = np.array(
        [p_left_bottom, p_left_top, p_right_top, p_right_bottom],
        dtype=np.float64,
    )

    return pages


pages = make_4_boxes(
    global_minx,
    global_miny,
    global_maxx,
    global_maxy,
    lsg_centroid_x,
    lsg_centroid_y,
)


@njit
def contains(box, point):
    minx, miny, maxx, maxy = box
    px, py = point

    if px > minx and px < maxx and py > miny and py < maxy:
        return True
    else:
        return False


@njit
def measure(pages, test_point):
    for box in pages:
        res = contains(box, test_point)


@njit
def blitz(centroid_arr, pages):

    for i in prange(len(centroid_arr)):
        point = centroid_arr[i]
        resval = measure(pages, point)

    return None


ticker = []
r = 1000000
for i in range(r):
    t1 = timeit.default_timer()
    res = blitz(centroid_arr, pages)
    t2 = timeit.default_timer() - t1
    ticker.append(t2)

print(f"\nTimes for {r:,} loops and {r * len(centroid_arr):,} equationss")
print(
    f"MIN:\t\t{round(min(ticker) * 1000000, 9):.9f} µs",
    f"\t\t{round(min(ticker) * 1000, 9):.9f} ms",
    f"\t\t{round(min(ticker), 9):.9f}",
    "s",
)
print(
    f"MAX:\t\t{round(max(ticker) * 1000000, 9):.4f} µs",
    f"\t\t{round(max(ticker) * 1000, 9):.7f} ms",
    f"\t\t{round(max(ticker), 9):.9f}",
    "s",
)
print(
    f"AVG:\t\t{round(sum(ticker) / len(ticker) * 1000000, 9):.9f} µs",
    f"\t\t{round(sum(ticker) / len(ticker) * 1000, 9):.9f} ms",
    f"\t\t{round(sum(ticker) / len(ticker), 9):.9f}",
    "s",
)
print(f"SUM:\t\t{round(sum(ticker), 9):.9f} s")
print(
    f"Avg. time per equations {round(sum(ticker) / len(ticker) * 1000000000 / len(centroid_arr), 9)} ns"
)
