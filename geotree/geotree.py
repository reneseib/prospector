import warnings

warnings.simplefilter("ignore", UserWarning)


import fiona
import timeit
from shapely.geometry import Polygon, MultiPolygon
import numpy as np
import pandas as pd
import os
import sys
import numba
from numba import njit, prange, typeof, typed, types
from numba.typed import Dict
from itertools import chain


"""
The general idea to improve computation time for the minimum distance
is to first check if our point of origin (the point we provide) is within
a TREE branch/leaf.

If it is not, we don't need to follow up searching within it and therefore
save the many computation within this leaf.

if it is within the TREE branch/leaf, we go to that branch and test if our
point is within the sub-branches/sub-leafs.

Repeat until lowest level and only then calculate the distances,
get the minimum distance and return it.
"""


# Setup
t1 = timeit.default_timer()
geo_file = "/common/ecap/prospector_data/results/stages/3-filtered_by_intersection_protected_area/bayern/gpkg/bayern-3-filtered_by_intersection_protected_area.gpkg"

lsg_file = (
    "/common/ecap/prospector_data/src_data/protected_areas/gpkg/lsg_gesamt_de.gpkg"
)


@njit
def hsplit(input, pos1, pos2):
    x1 = pos1
    x2 = pos2 + 1
    cols = x2 - x1
    res = np.empty((len(input), cols)).astype(input.dtype)
    for i in range(len(input)):
        row = input[i]
        res[i] = row[x1:x2]
    return res


@njit
def split_bbox_input(input):

    xcol = hsplit(input, 0, 0)

    ycol = hsplit(input, 1, 1)

    minx = min(xcol)
    miny = min(ycol)
    maxx = max(xcol)
    maxy = max(ycol)
    return minx, miny, maxx, maxy


def get_bbox(input):
    # bbox_vals =
    # minx, miny, maxx, maxy = bbox_vals

    return np.array(split_bbox_input(input))


@njit
def construct_true_bbox(bbox_arr):
    minx, miny, maxx, maxy = bbox_arr

    p1 = [minx, miny]
    p2 = [minx, maxy]
    p3 = [maxx, maxy]
    p4 = [maxx, miny]

    bbox = [p1, p2, p3, p4]

    return bbox


@njit
def get_centroid(x):
    cx, cy = np.sum(x, axis=0)
    cx = cx / len(x)
    cy = cy / len(x)
    result = np.array([cx, cy]).astype(np.float64)
    return result


@njit
def get_distance(p1, p2):
    x1, y1 = p1
    x2, y2 = p2

    a = abs(x1 - x2)
    b = abs(y1 - y2)
    c = np.sqrt(a ** 2 + b ** 2)
    return c


@njit
def within_bbox(bounds, point):
    minx, miny, maxx, maxy = bounds
    px, py = point

    if px >= minx and px <= maxx and py >= miny and py <= maxy:
        return 1
    else:
        return 0


@njit
def overlaps(bounds, bbox):
    # bboxes always has a length of 4 so it's safe to go directly with int(4)
    bbox_len = 4

    _results = np.zeros(bbox_len, dtype=np.int16)

    for i in range(bbox_len):
        point = bbox[i]
        _results[i] = within_bbox(bounds, point)

    bin_count = np.bincount(_results)
    if len(bin_count) == 2:
        if np.bincount(_results)[1] >= 1:
            return True

    return False


# @njit
def proc_polygons(input):

    bbox = get_bbox(input)
    centroid = get_centroid(input)

    return bbox, centroid


def proc_geometry(dataset):
    bbox, centroid = proc_polygons(dataset["coords"])
    dataset["bbox"] = bbox
    dataset["centroid"] = centroid

    return dataset


def prepare_data(geo_file):
    with fiona.open(geo_file) as geo:
        # REGULAR
        data = {}

        for i in range(len(geo)):
            feature = geo[i]
            if feature is not None:
                ################################################################
                # The following has to be later built as a function
                ################################################################
                orig_coords = feature["geometry"]["coordinates"][0]
                type = feature["geometry"]["type"]

                id = int(feature["id"])
                # Regular dict filling
                data[id] = {}
                data[id]["type"] = feature["geometry"]["type"]

                # Handle Polygons and MultiPolygons differently
                if type == "Polygon":
                    coords_len = len(orig_coords)
                    coords = (
                        np.array([np.array(x).astype(np.float64) for x in orig_coords])
                        .astype(np.float64)
                        .reshape(coords_len, 2)
                    )

                    if coords is not None:
                        data[id]["coords"] = coords

                else:
                    # List to store all np.arrayed lists in
                    coords_collection = []
                    # List to store all lengths of lists in list to get
                    # the total count of points in multipolygon
                    mp_coords_flat_len = []

                    # Iterate through list of list and append len of sublist
                    for i in range(len(orig_coords)):
                        mp_coords_flat_len.append(len(orig_coords[i]))

                    # Get the total count of all points
                    flat_len = sum(mp_coords_flat_len)

                    # Loop through all subarrays and transform all points to
                    # np.arrays
                    for i in range(len(orig_coords)):
                        mp_coords = orig_coords[i]
                        coords_len = len(mp_coords)
                        # Subcoords = Polygon in MultiPolygon
                        subcoords = (
                            np.array(
                                [np.array(x).astype(np.float64) for x in mp_coords]
                            )
                            .astype(np.float64)
                            .reshape(coords_len, 2)
                        )

                        # Append the new np.array of Point np.arrays to list
                        coords_collection.append(subcoords)

                    # Flatten the list to a single long np.array
                    coords = np.array(list(chain(*coords_collection))).astype(
                        np.float64
                    )

                    if coords is not None:
                        data[id]["coords"] = coords
    return data


def process_geodata(geo_file):

    ts_process_geodata = timeit.default_timer()

    geo_data = prepare_data(geo_file)

    te_process_geodata = timeit.default_timer() - ts_process_geodata
    print(f"function 'prepare_data': {te_process_geodata} sec")

    ts_proc_geometry = timeit.default_timer()
    for i in geo_data.keys():
        geo_data[i] = proc_geometry(geo_data[i])

    te_proc_geometry = timeit.default_timer() - ts_proc_geometry
    print(f"Loop with 'proc_geometry': {te_proc_geometry} sec")

    return geo_data


def make_leafs(global_bbox, global_centroid, buffer=1):
    # Since the syntax for boxes are minx, miny, maxx, maxy the 'left bottom'
    # and 'right top' can be retrieved from the existing coordinates.
    # Left top and right bottom have to be put together since the coordinates
    # can not be taken from the existing ones.

    bbox_minx, bbox_miny, bbox_maxx, bbox_maxy = global_bbox

    # Buffer (optional; a float number as percentage points; 1 = 100%)
    # that can enlarge or shrink the leafs by the value. Defaults to 1.
    # Example: buffer = 1.1 = 110% = +10%

    min_buffer = 1 - buffer + 1
    max_buffer = buffer

    bbox_minx = bbox_minx * min_buffer
    bbox_miny = bbox_miny * min_buffer
    bbox_maxx = bbox_maxx * max_buffer
    bbox_maxy = bbox_maxy * max_buffer

    centroid_x, centroid_y = global_centroid

    p_left_bottom = np.array([bbox_minx, bbox_miny, centroid_x, centroid_y]).astype(
        np.float64
    )

    p_right_top = np.array([centroid_x, centroid_y, bbox_maxx, bbox_maxy]).astype(
        np.float64
    )

    # Put the coordinates together for 'left top'
    p_left_top_minx = bbox_minx
    p_left_top_miny = centroid_y
    p_left_top_maxx = centroid_x
    p_left_top_maxy = bbox_maxy

    # Put the coordinates together for 'right bottom'
    p_right_bottom_minx = centroid_x
    p_right_bottom_miny = bbox_miny
    p_right_bottom_maxx = bbox_maxx
    p_right_bottom_maxy = centroid_y

    p_left_top = np.array(
        [p_left_top_minx, p_left_top_miny, p_left_top_maxx, p_left_top_maxy],
    ).astype(np.float64)

    p_right_bottom = np.array(
        [
            p_right_bottom_minx,
            p_right_bottom_miny,
            p_right_bottom_maxx,
            p_right_bottom_maxy,
        ]
    ).astype(np.float64)

    # Make a returnable leafs array with all 4 leafs
    # THE ORDER IS SUPER IMPORTANT
    leafs = np.array([p_left_bottom, p_left_top, p_right_top, p_right_bottom]).astype(
        np.float64
    )

    return leafs


regio_data = process_geodata(geo_file)

RD = {
    "data": regio_data,
}

pa_data = process_geodata(lsg_file)

# Variable that holds all information & calculation results
# to the respective file
PA = {
    "data": pa_data,
}


# pa_all_coords__packed = [np.array(pa_data[key]["coords"]) for key in pa_data.keys()]
#
# pa_all_coords__flat = list(chain(*pa_all_coords__packed))
# pa_all_coords__flat = list(chain(*pa_all_coords__flat))
# pa_all_coords__dissolved = np.array(pa_all_coords__flat).reshape(
#     int((len(pa_all_coords__flat) / 2)), 2
# )
#
# pa_global_centroid = get_centroid(pa_all_coords__dissolved)
# pa_global_bbox = get_bbox(pa_all_coords__dissolved)


rd_all_coords__packed = [
    np.array(regio_data[key]["coords"]) for key in regio_data.keys()
]

rd_all_coords__flat = list(chain(*rd_all_coords__packed))
rd_all_coords__flat = list(chain(*rd_all_coords__flat))
rd_all_coords__dissolved = np.array(rd_all_coords__flat).reshape(
    int((len(rd_all_coords__flat) / 2)), 2
)

rd_global_centroid = get_centroid(rd_all_coords__dissolved)
rd_global_bbox = get_bbox(rd_all_coords__dissolved)


# Set up the TREE with all its data
"""
MAJOR THOUGHT ERROR!!!!!!!!!!!!!!!!!

We need to make the pages within the boundaries of the RD region!
The global boundary is the bbox of the region and not the prot area!

"""
TREE = {
    "base": "PA",
    # "leafs": make_leafs(pa_global_bbox, pa_global_centroid, buffer=1.025),
    "leafs": make_leafs(rd_global_bbox, rd_global_centroid, buffer=1.025),
    "collections": {
        "parent": {
            0: [],
            1: [],
            2: [],
            3: [],
        },
        "child": {
            0: [],
            1: [],
            2: [],
            3: [],
        },
    },
}


# Next we need to find out, on which leaf our 'point to check' is.
# That way we can determine which leaf's polygon ids to load,
# to then load the respective data to run against our point.
def get_leaf_to_load(TREE, point):
    for i in range(len(TREE["leafs"])):
        leaf = TREE["leafs"][i]
        leaf_bounds = np.array(leaf).astype(np.float64)

        res = within_bbox(leaf_bounds, point)
        if res == 1:
            return i


# Now we add the polygons to the leaf, where the test_point is within
# that way we save time as we don't need to check all polygons first - only
# to not need 3/4 of it in the next step.


def get_leaf_collection(TREE, PA, leaf_to_load):
    """
    Arguments:
    TREE: to get the leafs
    PA: the bboxes we want to check for intersections with the leafs
    leaf_to_load: the leaf we want to check intersections with.

    Returns a list with the polygon ids of the polygons that
    are lying within the leaf.
    """
    bounds = TREE["leafs"][leaf_to_load]
    # Then we iterate over the polygons
    leaf_polygon_ids = []
    for k, v in PA["data"].items():
        # Iterate over the polygons of Protected Areas
        # and assign them to the leaf they are within.
        pa_item = PA["data"][k]
        pa_item_bbox = pa_item["bbox"]
        pa_item_bbox_points = construct_true_bbox(pa_item_bbox)
        pa_item_bbox_points = np.array(pa_item_bbox_points).astype(np.float64)
        res = overlaps(bounds, pa_item_bbox_points)
        if res == True:
            # Add this bbox's id (aka pointer to the polygon)
            # to the "leaf_contains" so we know which leaf
            # holds which polygons.
            leaf_polygon_ids.append(k)

    return leaf_polygon_ids


t2 = timeit.default_timer() - t1
print("Pre-processing took: ", t2, "seconds")


tgo = timeit.default_timer()


def raw_distances(RD, PA):
    """
    Arguments:
    RD:     The data with geometries we want to find the distances for.
    PA:     The data with geometries that are the distant objects.
    TREE:   The tree with leafs that group the distant objects

    Returns:
    A 2D numpy array that contains the polygon IDs and the polygon's
    'centroid-centroid-distances' to the respective closest object.

    Example:

       ids           distances
        ↓                ↓
    [[  1.       ,  73.8712483]
     [  2.       , 173.8712483]
               ...
     [  9.       , 873.8712483]
     [ 10.       , 973.8712483]]

    dtype=np.float64

    """

    # Get the centroids of all the geometries that we want to measure
    # a distance for. Here: the centroids of the 'RD Regio Data' polygons
    for key in list(RD["data"].keys()):
        rd_centroid = RD["data"][key]["centroid"]

        # Then we iterate through all the centroids, for each
        # measuring the distances within the respective tree leaf.
        # for rd_centroid in centroids_arr:
        # Set 'TREE' global as we don't want all leaf calculations
        # to repeat every time we start the function.
        global TREE

        leaf_to_load = get_leaf_to_load(TREE, rd_centroid)

        # First, collect the leafs for the parent; here PA
        if len(TREE["collections"]["parent"][leaf_to_load]) == 0:
            TREE["collections"]["parent"][leaf_to_load] = get_leaf_collection(
                TREE, PA, leaf_to_load
            )

        # Then collect the leafs for the child; here RD
        if len(TREE["collections"]["child"][leaf_to_load]) == 0:
            TREE["collections"]["child"][leaf_to_load] = get_leaf_collection(
                TREE, RD, leaf_to_load
            )

        # Once we have the collections, we check the distances
        # to the centroids of all polygons.
        # 1. Get all polygon ids
        polygon_ids = np.array(TREE["collections"]["parent"][leaf_to_load]).astype(
            np.int64
        )

        # Create empty 'result 2D array' in which we store
        # the values as [ID, DISTANCE]
        id_distances_arr = np.empty((len(polygon_ids), 2)).astype(np.float64)

        # Prepare an array with all centroids for all polygons
        # within that leaf
        pa_centroids = np.array(
            [PA["data"][id]["centroid"] for id in list(PA["data"].keys())]
        ).astype(np.float64)

        for i in prange(len(polygon_ids)):
            id = polygon_ids[i]
            pa_centroid = pa_centroids[i]
            p2pa_distance = get_distance(pa_centroid, rd_centroid)
            id_distances_arr[i] = np.array([id, p2pa_distance])

        # 5. Sort the id_distances_arr by ascending distance (column [1])
        sorted_id_distances_arr = id_distances_arr[id_distances_arr[:, 1].argsort()]

        # 6. Return the n-th shortest distances
        n = 10
        nth_sorted_id_distances = sorted_id_distances_arr[:n]

        # Load the real polygon points from the IDs of the shortlist
        closest_pa_ids = np.array([x[0] for x in nth_sorted_id_distances]).astype(
            np.float64
        )

        shortest_distances = []

        rd_polygon_ids_on_this_leaf = np.array(
            TREE["collections"]["child"][leaf_to_load]
        ).astype(np.float64)

        # Variables to prepare to speed up the whole looping
        # rd_data_coords = holds all polygon coords
        # Since we don't have a same length on all arrays, we can't
        # store them in another array. Therefore, we might revert to
        # a numba.TypedDict where key=id and value=array

        print("building typed dict")
        rd_data_coords = Dict.empty(
            key_type=types.int64,
            value_type=types.float64[:],
        )
        for id in rd_polygon_ids_on_this_leaf:
            rd_data_coords[id] = np.array([1, 2, 3]).astype(np.float64)

        print("typed dict:")
        print(len(rd_data_coords.keys()))
        print("\n\n")
        for k in range(len(rd_polygon_ids_on_this_leaf)):
            print("checking polygon id:", k)
            rd_id = rd_polygon_ids_on_this_leaf[k]
            orig_polygon = RD["data"][rd_id]["coords"]

            for i in range(len(closest_pa_ids)):
                pa_id = closest_pa_ids[i]
                pa_polygon_coords = PA["data"][pa_id]["coords"]

                polygon_single_distances = []

                for p1 in pa_polygon_coords:
                    for p2 in orig_polygon:
                        # print("p1", p1)
                        # print("p2", p2)
                        # print("")
                        # sys.exit()
                        d = get_distance(p1, p2)
                        polygon_single_distances.append(d)
                min_distance = min(polygon_single_distances)
                shortest_distances.append(min_distance)

        print(min(shortest_distances))

        return min(shortest_distances)


raw_distances = raw_distances(RD, PA)


tend = timeit.default_timer() - tgo
print(f"Actual distance calculations took {tend} sec")


# On these real points we iterate again for the distances

# And will eventually # retrieve the one with the min distance to our point

# This polygon incl. all information and the distance is then returned.
