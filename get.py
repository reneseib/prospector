import timeit
import random
from shapely.geometry import Point, Polygon, MultiPolygon, mapping
from shapely import wkt
import numpy as np
import geopandas as gpd
import os
import sys
from math import radians, cos, sin, asin, sqrt
from numba import njit
from multiprocessing import Process, Manager

# geoms = open("testdata.txt", "r").read()

file_name = "baden_wuerttemberg-3-filtered_by_intersection_protected_area.gpkg"
file_dir = "/common/ecap/prospector_data/results/stages/3-filtered_by_intersection_protected_area/baden_wuerttemberg/gpkg"

lsg_file = (
    "/common/ecap/prospector_data/src_data/protected_areas/gpkg/lsg_gesamt_de.gpkg"
)

file_path = os.path.join(file_dir, file_name)

data = gpd.read_file(file_path)

lsg = gpd.read_file(lsg_file)

gdf = gpd.GeoDataFrame(data).set_crs(25832, allow_override=True).to_crs(4326)
lsg_gdf = gpd.GeoDataFrame(lsg).set_crs(25832, allow_override=True).to_crs(4326)


gdf["geometry"] = gdf["geometry"].apply(
    lambda g: g.exterior.coords if type(g) == Polygon else g.convex_hull.exterior.coords
)


lsg_gdf["geometry"] = lsg_gdf["geometry"].apply(
    lambda g: g.exterior.coords if type(g) == Polygon else g
)

# Figure out what to do with multipolygons!
lsg_gdf["geometry"] = lsg_gdf["geometry"].apply(
    lambda g: g.exterior.coords
    if type(g) == Polygon
    else [x.exterior.coords for x in g]
    if type(g) == MultiPolygon
    else None
)


gdf_arr = [
    [np.array((Point(x).coords))[0] for x in gdf.iloc[i]["geometry"]]
    for i in range(len(gdf))
]

lsg_arr = [
    [np.array((Point(x).coords))[0] for x in lsg_gdf.iloc[i]["geometry"]]
    for i in range(len(lsg_gdf))
]

# print(lsg_arr)
# os._exit(3)


@njit(fastmath=True)
def haversine(p1, p2):
    """
    Calculate the great circle distance in kilometers between two points
    on the earth (specified in decimal degrees)

    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      IMPORTANT: ONLY WORKS WITH WSG84 / EPSG:4326 coordinates
    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    """

    if not np.array_equal(p1, p2):

        lon1, lat1 = p1[::-1]
        lon2, lat2 = p2[::-1]

        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
        c = 2 * np.arcsin(np.sqrt(a))
        r = 6371
        # r = Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
        return c * r


times = []

gdf["centroid"] = gdf["centroid"].apply(lambda x: wkt.loads(x))

tmp_gdf = gpd.GeoDataFrame(
    list(gdf["centroid"]), columns=["geometry"], geometry="geometry"
)
tmp_gdf = tmp_gdf.set_crs(25832).to_crs(4326)
gdf["centroid"] = tmp_gdf["geometry"]

i = 0
max_len = len(gdf["centroid"])

work_gdf = gdf[gdf["lsg_overlap"] == False]
remain_gdf = gdf[gdf["lsg_overlap"] == True]


def get_distance(gdf_arr, point, times):
    # print("starting cycle")
    start = timeit.default_timer()

    dists = []

    for p in gdf_arr:

        p = np.array([p[0][0], p[0][1]])
        distance = haversine(p, point)
        dists.append(distance)

    end = timeit.default_timer() - start
    times.append(end)
    global i
    global max_len
    i += 1

    print(" " * os.get_terminal_size().columns, end="\r")
    print(f"{(i / max_len)*100} %", end="\r")
    # sys.stdout.flush()
    return min(dists)


work_gdf["nearest_lsg"] = gdf["centroid"].apply(
    lambda x: get_distance(lsg_arr, np.array(x.coords)[0], times)
)

print(work_gdf[work_gdf["nearest_lsg"] == work_gdf["nearest_lsg"].min()])
print(work_gdf[work_gdf["nearest_lsg"] == work_gdf["nearest_lsg"].max()])


# # import geopandas as gpd
# #
# # maindir = "/common/ecap/prospector_data/results/stages/"
# #
# # stages = [
# #     "2-added_centroids",
# #     "3-filtered_by_intersection_protected_area",
# #     "4-added_nearest_protected_area",
# # ]
# #
# # regions = [
# #     "baden_wuerttemberg",
# #     "bayern",
# #     "brandenburg",
# #     "berlin",
# #     "bremen",
# #     "hamburg",
# #     "hessen",
# #     "mecklenburg_vorpommern",
# #     "niedersachsen",
# #     "nordrhein_westfalen",
# #     "rheinland_pfalz",
# #     "saarland",
# #     "sachsen_anhalt",
# #     "sachsen",
# #     "schleswig_holstein",
# #     "thueringen",
# # ]
# #
# # # for stage in stages:
# # #     stage_path = os.path.join(maindir, stage)
# # #     for regio in regions:
# # #         regio_dir = os.path.join(stage_path, regio)
# # #
# # #         gpkg_dir = os.path.join(regio_dir, "gpkg")
# # #         for file in os.listdir(gpkg_dir):
# # #             file_path = os.path.join(gpkg_dir, file)
# # #             print(file_path)
# # #             os.remove(file_path)
