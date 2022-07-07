import warnings

warnings.simplefilter("ignore", UserWarning)

import fiona
import timeit
from shapely.geometry import Point, Polygon, MultiPolygon, mapping, MultiPoint, box
from shapely import wkt
import numpy as np
import geopandas as gpd
import pandas as pd
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


def get_bbox(x):
    minx, miny = np.amin(x, axis=0)
    maxx, maxy = np.amax(x, axis=0)
    return np.array([minx, miny, maxx, maxy], dtype=np.float64)


@njit
def get_centroid(x):
    cx, cy = np.sum(x, axis=0)
    cx = cx / len(x)
    cy = cy / len(x)
    return cx, cy


times = []
t1 = timeit.default_timer()
with fiona.open(geo_file) as geo:
    t2 = timeit.default_timer() - t1
    t31 = timeit.default_timer()
    print("Loaded file in: ", t2)

    for i in prange(len(geo)):
        tf = timeit.default_timer()
        feature = geo[i]

        if feature is not None:
            if feature["geometry"]["type"] == "Polygon":
                coords = np.array(
                    feature["geometry"]["coordinates"][0], dtype=np.float64
                )
                arr = np.array(coords, dtype=np.float64).reshape(len(coords), 2)

                # print(mybbox)
                # otherbbox = get_bbox(arr)
                # print(otherbbox)
                # print("-" * 30)
                bbox = get_bbox(arr)
                centroid = get_centroid(arr)
                tfe = timeit.default_timer() - tf
                times.append(tfe)
                # print("-" * 10)
                # print(bbox)
                # print(centroid)
                # print("")

print("Avg. calc time: ", ((sum(times) / len(times)) * 1000000), "ms")
t3 = timeit.default_timer() - t2
print("Executed in: ", t3 - t31)
# lsg_file = (
#     "/common/ecap/prospector_data/src_data/protected_areas/gpkg/lsg_gesamt_de.gpkg"
# )
#
#
# # ALL GEO DATA
# geo_data = gpd.read_file(geo_file)
# geo_gdf = gpd.GeoDataFrame(geo_data).set_crs(25832, allow_override=True).to_crs(4326)
#
#
# # Make geo centroid arr
# gdf_centroid = geo_gdf["geometry"].centroid
#
# # geometry_arr = np.empty((len(geo_gdf), 3))
# poly_gdf = gpd.GeoDataFrame(
#     geo_gdf["geometry"], columns=["geometry"], geometry="geometry"
# )
#
# poly_gdf["geo_arr"] = None
# poly_gdf["bbox"] = None
#
# for i in range(len(poly_gdf)):
#     geometry = poly_gdf.iloc[i, poly_gdf.columns.get_loc("geometry")]
#
#     if type(geometry) == Polygon:
#         poly_gdf.loc[i, "geo_arr"] = geometry
#     else:
#         poly_gdf.loc[i, "geo_arr"] = None
#
# poly_gdf["geo_arr"] = poly_gdf["geo_arr"].apply(
#     lambda x: np.array(x.exterior.coords, dtype=np.float64).reshape(
#         len(x.exterior.coords), 2
#     )
#     if x is not None
#     else None
# )
#
#
# def get_bbox(x):
#     minx, miny = np.amin(x, axis=0)
#     maxx, maxy = np.amax(x, axis=0)
#     return minx, miny, maxx, maxy
#
#
# poly_gdf["bbox"] = poly_gdf["geo_arr"].apply(
#     lambda x: get_bbox(x) if x is not None else None
# )
#
# print(poly_gdf["bbox"][2])
#
#
# # if type(geometry) == MultiPolygon:
# #     poly_gdf.iloc[i, poly_gdf.columns.get_loc("geometry")] = [
# #         [np.array(x, dtype=np.float64) for x in mapping(geom)["coordinates"]]
# #         for geom in geometry.geoms
# #         for x in mapping(geom)["coordinates"]
# #     ]
#
# # break
#
# # print(poly_gdf)
# # geometry_arr = gdf_geometry.map(lambda geometry: np.array(geometry))
# # gdf_geometry = np.array(
# #     [
# #         .apply(
# #             lambda geometry: np.array(
# #                 [
# #                     [
# #                         np.array(x, dtype=np.float64)
# #                         for x in mapping(geom)["coordinates"]
# #                     ]
# #                     for geom in geometry.geoms
# #                     for x in mapping(geom)["coordinates"]
# #                 ]
# #             )
# #             if type(geometry) == MultiPolygon
# #             else np.array(geometry.exterior.coords, dtype=np.float64)
# #         )
# #     ]
# # )
# # print(gdf_geometry[0])
# sys.exit()
# gdf_bbox = geo_gdf["geometry"].apply(lambda x: x.bounds)
#
# gdf_centroid = gdf_centroid.map(lambda x: np.array(x.xy))
#
# gdf_centroid = gdf_centroid.map(lambda x: np.array([x[0], x[1]], dtype=np.float64))
#
# centroid_arr = np.zeros((len(gdf_centroid), 2), dtype=np.float64)
#
# for i in prange(len(centroid_arr)):
#     centroid_arr[i] = gdf_centroid[i].reshape(
#         2,
#     )
#
# # Make geo bounds arr
# gdf_bounds = geo_gdf["geometry"].map(lambda x: np.array(x.bounds))
#
# bounds_arr = np.zeros((len(gdf_bounds), 4), dtype=np.float64)
#
# for i in prange(len(bounds_arr)):
#     bounds_arr[i] = gdf_bounds[i].reshape(
#         4,
#     )
#
# # a, b, c, d = bounds_arr[0]
# # print("bounds_arr[0]:")
# # print(a, b, c, d)
# # print("\n\n")
#
# print("GEO DATA LOADED")
#
#
# # LSG DATA
#
# lsg_data = gpd.read_file(lsg_file)
# lsg_gdf = gpd.GeoDataFrame(lsg_data).set_crs(25832, allow_override=True).to_crs(4326)
#
#
# def get_global_bounds(lsg_gdf):
#     # 1.0 Get total bounds of lsg_gdf, returns np.array of
#     # minx, miny, maxx, maxy that is broadcasted to our variables
#
#     (
#         lsg_global_bbox__minx,
#         lsg_global_bbox__miny,
#         lsg_global_bbox__maxx,
#         lsg_global_bbox__maxy,
#     ) = np.array(lsg_gdf["geometry"].total_bounds)
#     return [
#         lsg_global_bbox__minx,
#         lsg_global_bbox__miny,
#         lsg_global_bbox__maxx,
#         lsg_global_bbox__maxy,
#     ]
#
#
# lsg_global_bbox = get_global_bounds(lsg_gdf)
#
#
# def get_global_centroid(lsg_gdf):
#     # 2.0 Get centroid of whole lsg_gdf
#     # 2.1 Get centroids of of all geometries as Points
#     lsg_centroids = lsg_gdf["geometry"].apply(lambda x: Point(x.centroid))
#
#     # 2.2 Make a separate GDF out of the centroid series
#     centroid_gdf = gpd.GeoDataFrame(lsg_centroids)
#
#     # 2.3 Retrieve the centroid coordinates as np.array from the
#     gdf_centroid_point = list(MultiPoint(centroid_gdf.geometry).centroid.xy)
#     cntrd_px, cntrd_py = [x[0] for x in gdf_centroid_point]
#     return cntrd_px, cntrd_py
#
#
# lsg_cntrd_px, lsg_cntrd_py = get_global_centroid(lsg_gdf)
#
#
# print("LSG DATA LOADED")
#
# # 3.0 Make the top level 4 boxes/pages of the tree
# #     The boxes all start or end with the global centroid
#
# # Since the syntax for boxes are minx, miny, maxx, maxy the 'left bottom' and
# # 'right top' can be retrieved from the existing coordinates.
# # Left top and right bottom have to be calculated since the coordinates
# # can not be taken from the existing bbox.
#
#
# def make_init_tree(
#     bbox_minx,
#     bbox_miny,
#     bbox_maxx,
#     bbox_maxy,
#     centroid_x,
#     centroid_y,
# ):
#     # Since the syntax for boxes are minx, miny, maxx, maxy the 'left bottom'
#     # and 'right top' can be retrieved from the existing coordinates.
#     # Left top and right bottom have to be put together since the coordinates
#     # can not be taken from the existing ones.
#
#     p_left_bottom = np.array(
#         [bbox_minx, bbox_miny, centroid_x, centroid_y], dtype=np.float64
#     )
#
#     p_right_top = np.array(
#         [centroid_x, centroid_y, bbox_maxx, bbox_maxy], dtype=np.float64
#     )
#
#     # Put the coordinates together for 'left top'
#     p_left_top_minx = bbox_minx
#     p_left_top_miny = centroid_y
#     p_left_top_maxx = centroid_x
#     p_left_top_maxy = bbox_maxy
#
#     # Put the coordinates together for 'right bottom'
#     p_right_bottom_minx = centroid_x
#     p_right_bottom_miny = bbox_miny
#     p_right_bottom_maxx = bbox_maxx
#     p_right_bottom_maxy = centroid_y
#
#     p_left_top = np.array(
#         [p_left_top_minx, p_left_top_miny, p_left_top_maxx, p_left_top_maxy],
#         dtype=np.float64,
#     )
#
#     p_right_bottom = np.array(
#         [
#             p_right_bottom_minx,
#             p_right_bottom_miny,
#             p_right_bottom_maxx,
#             p_right_bottom_maxy,
#         ],
#         dtype=np.float64,
#     )
#
#     # Make a returnable pages array with all 4 pages
#     # THE ORDER IS SUPER IMPORTANT
#     pages = np.array(
#         [p_left_bottom, p_left_top, p_right_top, p_right_bottom],
#         dtype=np.float64,
#     )
#
#     return pages
#
#
# pages = make_init_tree(
#     lsg_global_bbox[0],
#     lsg_global_bbox[1],
#     lsg_global_bbox[2],
#     lsg_global_bbox[3],
#     lsg_cntrd_px,
#     lsg_cntrd_py,
# )
#
#
# @njit
# def within(bounds, point):
#     minx, miny, maxx, maxy = bounds
#     px, py = point
#
#     if px >= minx and px <= maxx and py >= miny and py <= maxy:
#         return 1
#     else:
#         return 0
#
#
# @njit
# def contains(bounds, bbox):
#     _results = np.zeros(len(points), dtype=np.int16)
#
#     for i in 4:
#         point = bbox[i]
#         _results[i] = within(bounds, point)
#
#     if np.bincount(_results)[1] == 4:
#         return True
#
#     return False
#
#
# @njit
# def overlaps(bounds, bbox):
#     # bboxes always has a length of 4 so it's safe to go directly with int(4)
#     bbox_len = 4
#
#     _results = np.zeros(bbox_len, dtype=np.int16)
#
#     for i in range(bbox_len):
#         point = bbox[i]
#         _results[i] = within(bounds, point)
#
#     if np.bincount(_results)[1] == 2:
#         return True
#
#     return False
#
#
# @njit
# def allocate_geomids_to_pages(pages, bbox):
#     """
#     This function allocates geometry IDs to the tree's pages and subpages.
#     """
#     pages_len = 4
#     # Create empty array of 4 to store the results of each
#     # page check at the given position
#     results = np.zeros(pages_len, dtype=np.int16)
#
#     for p in range(pages_len):
#         bounds = pages[p]
#
#         contains = contains(bounds, bbox)
#         overlaps = overlaps(bounds, bbox)
#         if contains_check:
#             results[p] = p
#
#         else:
#             # We need an integer >3 (since 0-3 pages) as error value
#             # since it is an integer array
#             results[p] = 9
#
#     return results
#
#
# @njit
# def blitz(centroid_arr, pages):
#     arr_len = len(centroid_arr)
#
#     results = np.zeros(
#         (arr_len, 4), dtype=np.int32
#     )  # Here use int32 since we don't know the length of centroid_arr
#
#     for i in prange(arr_len):
#         bbox = centroid_arr[i]
#         results[i] = allocate_geomids_to_pages(pages, bbox)
#
#     return results
#
#
# ticker = []
# r = 1
# for i in range(r):
#     t1 = timeit.default_timer()
#
#     res = blitz(centroid_arr, pages)
#
#     t2 = timeit.default_timer() - t1
#     ticker.append(t2)
#
# print(f"\nTimes for {r:,} loops and {r * len(centroid_arr):,} equationss")
# print(
#     f"MIN:\t\t{round(min(ticker) * 1000000, 9):.9f} µs",
#     f"\t\t{round(min(ticker) * 1000, 9):.9f} ms",
#     f"\t\t{round(min(ticker), 9):.9f}",
#     "s",
# )
# print(
#     f"MAX:\t\t{round(max(ticker) * 1000000, 9):.4f} µs",
#     f"\t\t{round(max(ticker) * 1000, 9):.7f} ms",
#     f"\t\t{round(max(ticker), 9):.9f}",
#     "s",
# )
# print(
#     f"AVG:\t\t{round(sum(ticker) / len(ticker) * 1000000, 9):.9f} µs",
#     f"\t\t{round(sum(ticker) / len(ticker) * 1000, 9):.9f} ms",
#     f"\t\t{round(sum(ticker) / len(ticker), 9):.9f}",
#     "s",
# )
# print(f"SUM:\t\t{round(sum(ticker), 9):.9f} s")
# print(
#     f"Avg. time per equations {round(sum(ticker) / len(ticker) * 1000000000 / len(centroid_arr), 9)} ns"
# )
