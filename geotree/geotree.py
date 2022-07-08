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
a tree branch/page.

If it is not, we don't need to follow up searching within it and therefore
save the many computation within this page.

if it is within the tree branch/page, we go to that branch and test if our
point is within the sub-branches/sub-pages.

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


def make_init_tree(global_bbox, global_centroid, buffer=1):
    # Since the syntax for boxes are minx, miny, maxx, maxy the 'left bottom'
    # and 'right top' can be retrieved from the existing coordinates.
    # Left top and right bottom have to be put together since the coordinates
    # can not be taken from the existing ones.

    bbox_minx, bbox_miny, bbox_maxx, bbox_maxy = global_bbox

    # Buffer (optional; a float number as percentage points; 1 = 100%)
    # that can enlarge or shrink the pages by the value. Defaults to 1.
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

    # Make a returnable pages array with all 4 pages
    # THE ORDER IS SUPER IMPORTANT
    pages = np.array(
        [p_left_bottom, p_left_top, p_right_top, p_right_bottom],
        dtype=np.float64,
    )

    return pages


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


# Now comes pagination
pa_init_tree = make_init_tree(pa_global_bbox, pa_global_centroid, buffer=1.05)

# Set up the tree with all its data
tree = {
    "init_tree": pa_init_tree,
    "collections": {
        0: [],
        1: [],
        2: [],
        3: [],
    },
}

# Now we add the polygons to the page, where the test_point is within
# that way we save time as we don't need to check all polygons first just
# to not need 3/4 of it in the next step.
i = -1
for k, v in PA["data"].items():
    # Iterate over the polygons of Protected Areas
    # and assign them to the page they are within.
    pa_item = PA["data"][k]
    pa_item_bbox = pa_item["bbox"]
    pa_item_bbox_points = construct_true_bbox(pa_item_bbox)
    for i in range(len(tree["init_tree"])):
        bounds = tree["init_tree"][i]
        res = overlaps(bounds, pa_item_bbox_points)
        if res == True:
            # Add this bbox id (pointer to the polygon) to the "page_contains"
            # so we know which page holds which polygons.
            tree["collections"][i].append(k)


# Next we need to find out, on which page our point to check is.
# That way we can determine which page's polygon ids to load,
# to then load the respective data to run against our point.
orig_point = np.array([666666.6666, 5613500.6666]).astype(np.float64)


def get_page_to_load(bounds_dict, point):
    page_to_load = None
    for i in range(len(bounds_dict)):
        bounds = np.array(bounds_dict[i]).astype(np.float64)

        res = within(bounds, orig_point)
        if res == 1:
            page_to_load = i
            return page_to_load


page_to_load = get_page_to_load(tree["init_tree"], orig_point)


print("We need to load page: ", page_to_load)


t2 = timeit.default_timer() - t1
print("Data processing: ", t2, "seconds")
