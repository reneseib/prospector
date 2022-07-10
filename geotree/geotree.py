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
from numba.typed import Dict, List
from itertools import chain


"""
The general idea to improve the minimum distance calculation time is to first check if our point (the point we specified) is inside a 'tree leaf'. Tree leaves are the result of combining the vertices of the global BBOX and a weighted center point, the global centroid of our dataset.

If this is not the case, we do not need to continue the search and therefore we can save the many calculations inside this 'leaf'.

If it is inside the 'tree leaf', we go to that leaf and check if our point is inside the sub-leaves.

We repeat this to the lowest level and only then calculate the distances, determine the minimum value from it and return it.
"""


# Setup
t1 = timeit.default_timer()
geo_file = "/common/ecap/prospector_data/results/stages/3-filtered_by_intersection_protected_area/bremen/gpkg/bremen-3-filtered_by_intersection_protected_area.gpkg"

lsg_file = (
    "/common/ecap/prospector_data/src_data/protected_areas/gpkg/nsg_gesamt_de.gpkg"
)


@njit
def column_split(input, pos1, pos2):
    x1 = pos1
    x2 = pos2 + 1
    cols = x2 - x1
    res = List()
    for i in range(len(input)):
        row = input[i]
        res.append(row[x1:x2])
    return res


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
def get_bbox(input):
    xcol = column_split(input, 0, 0)
    ycol = column_split(input, 1, 1)

    minx = min(xcol)
    miny = min(ycol)
    maxx = max(xcol)
    maxy = max(ycol)

    return List([minx, miny, maxx, maxy])


@njit
def get_centroid(input):
    cx, cy = np.sum(input, axis=0)
    cx = cx / len(input)
    cy = cy / len(input)
    result = List([cx, cy])
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


@njit
def make_bbox_centroid(input):

    bbox = get_bbox(input)
    centroid = get_centroid(input)

    return bbox, centroid


@njit
def flatten_dict_elements(coords, keys):
    collect_list = List()
    for i in coords.keys():
        geom = coords[i]
        for g in geom:
            collect_list.append(g)

    return collect_list


@njit
def assign_bbox_to_id(int_arr_dict, geo_data, id, bbox):
    bbox_int_arr_dict = int_arr_dict.copy()

    bbox_int_arr_dict[0] = bbox
    geo_data[id]["bbox"] = bbox_int_arr_dict

    return geo_data


@njit
def assign_centroid_to_id(int_arr_dict, geo_data, id, centroid):
    centroid_int_arr_dict = int_arr_dict.copy()

    centroid_int_arr_dict[0] = List([centroid])
    geo_data[id]["centroid"] = centroid_int_arr_dict

    return geo_data


@njit
def proc_geometry(geo_data, int_arr_dict):
    for i in geo_data.keys():
        coords = geo_data[i]["coords"]
        coords = coords[0]

        bbox, centroid = make_bbox_centroid(coords)

        # Add bboxes to the geo_data and return updated geo_data
        geo_data = assign_bbox_to_id(int_arr_dict, geo_data, i, bbox)

        # Add centroid to the geo_data and return updated geo_data
        geo_data = assign_centroid_to_id(int_arr_dict, geo_data, i, centroid)

    return geo_data


def create_typed_dicts():
    nb_list = List.empty_list(item_type=types.int64)

    nb_float_list = List.empty_list(item_type=types.float64)
    nb_nested_list = List([nb_float_list, nb_float_list])

    int_arr_dict = Dict.empty(key_type=types.int64, value_type=typeof(nb_nested_list))

    str_arr_dict = Dict.empty(
        key_type=types.unicode_type,
        value_type=typeof(int_arr_dict),
    )

    data_dict = Dict.empty(
        key_type=types.int64,
        value_type=typeof(str_arr_dict),
    )
    return int_arr_dict, str_arr_dict, data_dict


def prepare_data(geo_file, int_arr_dict, str_arr_dict, data_dict):
    with fiona.open(geo_file) as geo:
        # EXPERIMENTAL

        geo = dict(geo)

        data_keys = list(geo.keys())
        geometries = {}
        for k in data_keys:
            coords = geo[k]["geometry"]["coordinates"]
            geometries[k] = coords

        for key in data_keys:
            geom = geometries[key]
            geom = list(chain(*geom))
            if type(geom[0]) == list:
                geom = list(chain(*geom))

            geom = List([List(x) for x in geom])

            # Copy the typed dictionaries for each geometry
            int_arr_dict = int_arr_dict.copy()
            str_arr_dict = str_arr_dict.copy()

            # lgeom = List(geom)
            # print(lgeom)
            # sys.exit()

            int_arr_dict[0] = geom

            str_arr_dict["coords"] = int_arr_dict

            data_dict[key] = str_arr_dict

        return data_dict
    # EXPERIMENTAL END


def process_geodata(geo_file):

    timer_start = timeit.default_timer()

    int_arr_dict, str_arr_dict, data_dict = create_typed_dicts()

    geo_data = prepare_data(geo_file, int_arr_dict, str_arr_dict, data_dict)

    timer_end = timeit.default_timer() - timer_start
    print(f"function 'prepare_data': {timer_end} sec")

    timer_start = timeit.default_timer()

    # for i in geo_data.keys():
    #     geo_data[i] = proc_geometry(geo_data)
    geo_data = proc_geometry(geo_data, int_arr_dict)
    timer_end = timeit.default_timer() - timer_start
    print(f"Loop with 'proc_geometry': {timer_end} sec")

    return geo_data


def make_leaves(global_bbox, global_centroid, buffer=1):
    # Since the syntax for boxes are minx, miny, maxx, maxy the 'left bottom'
    # and 'right top' can be retrieved from the existing coordinates.
    # Left top and right bottom have to be put together since the coordinates
    # can not be taken from the existing ones.

    bbox_minx, bbox_miny, bbox_maxx, bbox_maxy = np.array(global_bbox).flatten()

    # Buffer (optional; a float number as percentage points; 1 = 100%)
    # that can enlarge or shrink the leaves by the value. Defaults to 1.
    # Example: buffer = 1.1 = 110% = +10%

    min_buffer = 1 - buffer + 1
    max_buffer = buffer

    bbox_minx = bbox_minx * min_buffer
    bbox_miny = bbox_miny * min_buffer
    bbox_maxx = bbox_maxx * max_buffer
    bbox_maxy = bbox_maxy * max_buffer

    centroid_x, centroid_y = global_centroid

    p_left_bottom = List([bbox_minx, bbox_miny, centroid_x, centroid_y])

    p_right_top = List([centroid_x, centroid_y, bbox_maxx, bbox_maxy])

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

    p_left_top = List(
        [p_left_top_minx, p_left_top_miny, p_left_top_maxx, p_left_top_maxy],
    )

    p_right_bottom = List(
        [
            p_right_bottom_minx,
            p_right_bottom_miny,
            p_right_bottom_maxx,
            p_right_bottom_maxy,
        ]
    )

    # Make a returnable leaves array with all 4 leaves
    # THE ORDER IS SUPER IMPORTANT
    leaves = List([p_left_bottom, p_left_top, p_right_top, p_right_bottom])

    return leaves


# Variables that hold all information & calculation results
# to the respective file
RD = process_geodata(geo_file)

PA = process_geodata(lsg_file)


rd_all_coords__packed = [np.array(RD[key]["coords"][0]) for key in RD.keys()]

rd_all_coords__dissolved = np.array(list(chain(*rd_all_coords__packed)))

rd_global_centroid = get_centroid(rd_all_coords__dissolved)
rd_global_bbox = get_bbox(rd_all_coords__dissolved)


# Set up the TREE with all its data

# EXPERIMENTAL
nb_list = List.empty_list(item_type=types.int64)

collections_list_dict = Dict.empty(key_type=types.int64, value_type=typeof(nb_list))

TREE_collections = Dict.empty(
    key_type=types.unicode_type, value_type=typeof(collections_list_dict)
)

for i in range(4):
    collections_list_dict[i] = List.empty_list(item_type=types.int64)

TREE_collections["parent"] = collections_list_dict
TREE_collections["child"] = collections_list_dict

TREE_leaves = make_leaves(rd_global_bbox, rd_global_centroid, buffer=1.025)

# EXPERIMENTAL END


# Next we need to find out, on which leaf our 'point to check' is.
# That way we can determine which leaf's polygon ids to load,
# to then load the respective data to run against our point.
@njit
def get_leaf_to_load(TREE_leaves, point):
    for i in range(len(TREE_leaves)):
        leaf_bounds = TREE_leaves[i]
        # leaf_bounds = List(leaf)

        res = within_bbox(leaf_bounds, point)
        if res == 1:
            return i


# Now we add the polygons to the leaf, where the test_point is within
# that way we save time as we don't need to check all polygons first - only
# to not need 3/4 of it in the next step.
@njit
def collect_ids_in_leaf(PA, bounds):

    leaf_polygon_ids = List()
    for i in PA.keys():

        pa_item = PA[i]
        pa_item_bbox = pa_item["bbox"][0]

        pa_item_bbox = pa_item_bbox.flatten()

        minx, miny, maxx, maxy = pa_item_bbox

        p1 = List([minx, miny])
        p2 = List([minx, maxy])
        p3 = List([maxx, maxy])
        p4 = List([maxx, miny])

        bbox = List([p1, p2, p3, p4])
        #
        # pa_item_bbox_points = construct_true_bbox(pa_item_bbox)
        # pa_item_bbox_points = np.array(pa_item_bbox_points).astype(np.float64)
        res = overlaps(bounds, bbox)
        if res == True:
            # Add this bbox's id (aka pointer to the polygon)
            # to the "leaf_contains" so we know which leaf
            # holds which polygons.
            leaf_polygon_ids.append(i)

    return leaf_polygon_ids


@njit
def get_leaf_collection(TREE_leaves, PA, leaf_to_load):
    """
    Arguments:
    TREE: to get the leaves
    PA: the bboxes we want to check for intersections with the leaves
    leaf_to_load: the leaf we want to check intersections with.

    Returns a list with the polygon ids of the polygons that
    are lying within the leaf.
    """

    bounds = TREE_leaves[leaf_to_load]

    leaf_polygon_ids = collect_ids_in_leaf(PA, bounds)

    return leaf_polygon_ids


t2 = timeit.default_timer() - t1
print("Pre-processing took: ", t2, "seconds")


tgo = timeit.default_timer()


@njit(fastmath=True)
def calculate_shortest_distance(current_polygon, closest_pa_ids, PA):
    shortest_distances = List()

    for p1 in current_polygon:
        for i in range(len(closest_pa_ids)):
            pa_id = closest_pa_ids[i]
            pa_id = int(pa_id)

            pa_polygon_coords = PA[pa_id]["coords"][0]

            polygon_single_distances = []

            for p2 in pa_polygon_coords:
                d = get_distance(p1, p2)
                polygon_single_distances.append(d)
            min_distance = min(polygon_single_distances)
        shortest_distances.append(min_distance)

    return min(shortest_distances)


def calculate_min_distance(RD, PA):
    """
    Arguments:
    RD:     The data with geometries we want to find the distances for.
    PA:     The data with geometries that are the distant objects.
    TREE:   The tree with leaves that group the distant objects

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
    for key in list(RD.keys()):
        rd_centroid = RD[key]["centroid"]
        rd_centroid = rd_centroid[0][0]

        # Then we iterate through all the centroids, for each
        # measuring the distances within the respective tree leaf.
        # for rd_centroid in centroids_arr:
        # Set 'TREE' global as we don't want all leaf calculations
        # to repeat every time we start the function.
        global TREE_leaves
        leaf_to_load = get_leaf_to_load(TREE_leaves, rd_centroid)

        # First, collect the leaves for the parent; here PA
        if len(TREE_collections["parent"][leaf_to_load]) == 0:
            TREE_collections["parent"][leaf_to_load] = get_leaf_collection(
                TREE_leaves, PA, leaf_to_load
            )

        # Then collect the leaves for the child; here RD
        if len(TREE_collections["child"][leaf_to_load]) == 0:
            TREE_collections["child"][leaf_to_load] = get_leaf_collection(
                TREE_leaves, RD, leaf_to_load
            )

        # Once we have the collections, we check the distances
        # to the centroids of all polygons.
        # 1. Get all polygon ids
        polygon_ids = List(TREE_collections["parent"][leaf_to_load])

        # Create empty 'result 2D array' in which we store
        # the values as [ID, DISTANCE]
        id_distances_arr = np.empty((len(polygon_ids), 2)).astype(np.float64)

        # Prepare an array with all centroids for all polygons
        # within that leaf
        pa_centroids = np.array(
            [PA[id]["centroid"][0] for id in list(PA.keys())]
        ).astype(np.float64)

        for i in prange(len(polygon_ids)):
            id = polygon_ids[i]
            pa_centroid = pa_centroids[i][0]

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

        rd_polygon_ids_on_this_leaf = np.array(
            TREE_collections["child"][leaf_to_load]
        ).astype(np.float64)

        current_polygon = RD[key]["coords"][0]

        shortest_distance = calculate_shortest_distance(
            current_polygon, closest_pa_ids, PA
        )
        print(shortest_distance)

    return None


calculate_min_distance = calculate_min_distance(RD, PA)


tend = timeit.default_timer() - tgo
print(f"Actual distance calculations took {tend} sec")
