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
geo_file = "/common/ecap/prospector_data/results/stages/3-filtered_by_intersection_protected_area/baden_wuerttemberg/gpkg/baden_wuerttemberg-3-filtered_by_intersection_protected_area.gpkg"

lsg_file = (
    "/common/ecap/prospector_data/src_data/protected_areas/gpkg/lsg_gesamt_de.gpkg"
)


def get_bbox(x):
    minx, miny = np.amin(x, axis=0)
    maxx, maxy = np.amax(x, axis=0)
    result = np.array([minx, miny, maxx, maxy], dtype=np.float64)
    return result


def construct_true_bbox(bbox_arr):
    minx, miny, maxx, maxy = bbox_arr

    p1 = np.array([minx, miny])
    p2 = np.array([minx, maxy])
    p3 = np.array([maxx, maxy])
    p4 = np.array([maxx, miny])

    bbox = np.array([p1, p2, p3, p4]).astype(np.float64)

    return bbox


def get_centroid(x):

    if (
        type(x) is np.ndarray
        and type(x[0]) is np.ndarray
        and type(x[0][0]) is np.float64
    ):
        # It is a multipolygon
        cx, cy = np.sum(x, axis=0)
        cx = cx / len(x)
        cy = cy / len(x)
        result = np.array([cx, cy], dtype=np.float64)
        return result

    elif type(x) is np.ndarray and type([x0]) is np.float64:

        # It is a polygon
        cx, cy = np.sum(x, axis=0)
        cx = cx / len(x)
        cy = cy / len(x)
        result = np.array([cx, cy], dtype=np.float64)
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
def within(bounds, point):
    minx, miny, maxx, maxy = bounds
    px, py = point

    if px >= minx and px <= maxx and py >= miny and py <= maxy:
        return 1
    else:
        return 0


@njit
def contains(bounds, bbox):
    _results = np.zeros(len(points), dtype=np.int16)

    for i in 4:
        point = bbox[i]
        _results[i] = within(bounds, point)

    if np.bincount(_results)[1] == 4:
        return True

    return False


@njit
def overlaps(bounds, bbox):
    # bboxes always has a length of 4 so it's safe to go directly with int(4)
    bbox_len = 4

    _results = np.zeros(bbox_len, dtype=np.int16)

    for i in range(bbox_len):
        point = bbox[i]
        _results[i] = within(bounds, point)

    bin_count = np.bincount(_results)
    if len(bin_count) == 2:
        if np.bincount(_results)[1] >= 1:
            return True

    return False


def proc_multi_polygons(input):
    # MultiPolygons are nested arrays/lists.
    # Therefore we first need to flatten it to a single array
    # be able to get our two results: bbox, centroid.
    input_unpacked = list(chain(*input))
    dissolved_arr = np.array(input_unpacked)

    bbox = get_bbox(dissolved_arr)
    centroid = get_centroid(dissolved_arr)

    return bbox, centroid


def proc_polygons(input):
    dissolved_input = np.array(input)

    bbox = get_bbox(dissolved_input)
    centroid = get_centroid(dissolved_input)

    return bbox, centroid


def proc_geometry(dataset):
    if dataset["type"] == "MultiPolygon":
        bbox, centroid = proc_multi_polygons(dataset["coords"])
        dataset["bbox"] = bbox
        dataset["centroid"] = centroid

    if dataset["type"] == "Polygon":
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
                    coords_collection = []
                    for i in range(len(orig_coords)):
                        mp_coords = orig_coords[i]
                        coords_len = len(mp_coords)
                        subcoords = (
                            np.array(
                                [np.array(x).astype(np.float64) for x in mp_coords]
                            )
                            .astype(np.float64)
                            .reshape(coords_len, 2)
                        )
                        coords_collection.append(subcoords)

                    if coords_collection is not None:
                        data[id]["coords"] = coords_collection
    return data


def process_geodata(geo_file):
    geo_data = prepare_data(geo_file)

    for i in geo_data.keys():
        enriched_data = proc_geometry(geo_data[i])
        geo_data[i] = enriched_data
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

    p_left_bottom = np.array(
        [bbox_minx, bbox_miny, centroid_x, centroid_y], dtype=np.float64
    )

    p_right_top = np.array(
        [centroid_x, centroid_y, bbox_maxx, bbox_maxy], dtype=np.float64
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
        dtype=np.float64,
    )

    p_right_bottom = np.array(
        [
            p_right_bottom_minx,
            p_right_bottom_miny,
            p_right_bottom_maxx,
            p_right_bottom_maxy,
        ],
        dtype=np.float64,
    )

    # Make a returnable leafs array with all 4 leafs
    # THE ORDER IS SUPER IMPORTANT
    leafs = np.array(
        [p_left_bottom, p_left_top, p_right_top, p_right_bottom],
        dtype=np.float64,
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


pa_all_coords__packed = [np.array(pa_data[key]["coords"]) for key in pa_data.keys()]

pa_all_coords__flat = list(chain(*pa_all_coords__packed))
pa_all_coords__flat = list(chain(*pa_all_coords__flat))
pa_all_coords__dissolved = np.array(pa_all_coords__flat)

pa_global_centroid = get_centroid(pa_all_coords__dissolved)
pa_global_bbox = get_bbox(pa_all_coords__dissolved)


# Set up the TREE with all its data
TREE = {
    "leafs": make_leafs(pa_global_bbox, pa_global_centroid, buffer=1.025),
    "collections": {
        0: [],
        1: [],
        2: [],
        3: [],
    },
}


# Next we need to find out, on which leaf our 'point to check' is.
# That way we can determine which leaf's polygon ids to load,
# to then load the respective data to run against our point.
def get_leaf_to_load(TREE, point):
    for i in range(len(TREE["leafs"])):
        leaf = TREE["leafs"][i]
        leaf_bounds = np.array(leaf).astype(np.float64)

        res = within(leaf_bounds, point)
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
        res = overlaps(bounds, pa_item_bbox_points)
        if res == True:
            # Add this bbox's id (aka pointer to the polygon)
            # to the "leaf_contains" so we know which leaf
            # holds which polygons.
            leaf_polygon_ids.append(k)

    return leaf_polygon_ids


t2 = timeit.default_timer() - t1
print("Pre-processing took: ", t2, "seconds")

# Here we can iterate over the points that we want to find
# the closest pa_polygon for.

# # MAKE LOOP HERE OVER OUR POINTS
"""
Only test data
"""
# xs = np.random.uniform(296666, 915443, [10, 1])
# ys = np.random.uniform(5340858, 6068663, [10, 1])
#
# points_arr = np.column_stack((xs, ys))

tgo = timeit.default_timer()

"""
Replace points_arr with real data
"""
points_arr = [RD["data"][key]["centroid"] for key in list(RD["data"].keys())]
calcs = []


def raw_distances(
    RD,
    PA,
    TREE,
):
    """
    Arguments:
    RD:     The data with geometries we want to find the distances for.
    PA:     The data with geometries that are the distant objects.
    TREE:   The tree with leafs that group the distant objects

    Returns:
    A 2D numpy array that contains the polygon IDs and the polygon's
    'centroid-centroid-distance' to the closest object, for example:

      ids           distances
       ↓                ↓
    [  1.       ,  73.8712483]
    [  2.       , 173.8712483]
    ...
    [  9.       , 873.8712483]
    [ 10.       , 973.8712483]
    dtype=np.float64

    """
    # Get the centroids of all the geometries that we want to measure
    # a distance for. Here: the centroids of the 'RD Regio Data' polygons
    centroids_arr = np.array(
        [RD["data"][key]["centroid"] for key in list(RD["data"].keys())]
    ).astype(np.float64)

    # Then we iterate through all the centroids, for each
    # measuring the distances within the respective tree leaf.
    return None


for orig_point in points_arr:

    leaf_to_load = get_leaf_to_load(TREE, orig_point)

    if len(TREE["collections"][leaf_to_load]) == 0:
        TREE["collections"][leaf_to_load] = get_leaf_collection(TREE, PA, leaf_to_load)

    # Once we have the collections, we check the distances
    # to the centroids of all polygons.
    # 1. Get all polygon ids
    polygon_ids = np.array(TREE["collections"][leaf_to_load]).astype(np.int64)

    # Collect data for measurements
    calcs.append(len(polygon_ids))

    # 2. Setup a dict where all distances are stored in as
    #    {id: distance}
    point_to_pa_distances = {}

    # 3. Iterate over all polygon centroids and get the distance
    #    between centroid and orig_point, store distance in list

    # Result array where we store the values as [ID, DISTANCE]
    id_distances_arr = np.empty((len(polygon_ids), 2)).astype(np.float64)

    # Prepare an array with all centroids for all polygons of that leaf
    pa_centroids = np.array(
        [PA["data"][id]["centroid"] for id in list(PA["data"].keys())]
    ).astype(np.float64)

    for i in prange(len(polygon_ids)):
        id = polygon_ids[i]
        pa_centroid = pa_centroids[i]
        p2pa_distance = get_distance(pa_centroid, orig_point)
        id_distances_arr[i] = np.array([id, p2pa_distance])

    # 4. Get the minimum distance from the array
    #    >> np.amin(id_distances_arr, axis=0)[1]
    #       returns an array with min [0] IDs and [1] DISTANCES
    min_distance_meter = np.amin(id_distances_arr, axis=0)[1]

    # 5. Sort the id_distances_arr by ascending distance (column [1])

    # 6. Slice the first 10 elements from the sorted id_distances_arr
    #    to get the 10 centroid-closest polygon ids

    # 7.

    # """
    # CHANGE THAT TO NP ARRAY MECHANICS
    # """
    # min_distance_id_arr = np.where(id_distances_arr == min_distance_meter)
    # min_distance_id = min_distance_id_arr[0][0]
    # print("POINT: ", orig_point)
    # print("ID of closest protected area:", min_distance_id)
    # print("POLYGON DATA of closest protected area: ", PA["data"][min_distance_id])
    # sys.exit()


tend = timeit.default_timer() - tgo
print(f"{sum(calcs)} distance calculations took {tend} sec")


# We store them, find the lowest 10 distances, get the IDs to these centroids
# and load the real polygon points.

# On these real points we iterate again for the distances

# And will eventually # retrieve the one with the min distance to our point

# This polygon incl. all information and the distance is then returned.
