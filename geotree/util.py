import sys
import os
import numpy as np
import numba
from numba import njit, prange, typeof, typed, types
from numba.typed import Dict, List
from shapely.geometry import Point, Polygon, MultiPolygon, LineString, MultiLineString


class style:
    PURPLE = "\033[95m"
    CYAN = "\033[96m"
    DARKCYAN = "\033[36m"
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    END = "\033[0m"

    LINE_UP = "\033[1A"
    LINE_CLEAR = "\x1b[2K"


@njit(fastmath=True)
def get_distance(p1, p2):
    x1, y1 = p1
    x2, y2 = p2

    a = abs(x1 - x2)
    b = abs(y1 - y2)
    c = np.sqrt(a ** 2 + b ** 2)
    return c


@njit(fastmath=True, parallel=True)
def calculate_shortest_distance(current_polygon, closest_pa_ids, PA):
    shortest_distances = List()

    # CHECK IF ANY POINT IN CURRENT_POLYGON IS IDENTICAL WITH
    # A POINT OF THE TARGET POLYGON. IF YES, WE DON'T NEED TO CALCULATE
    # BECAUSE THEY INTERSECT AND DISTANCE IS ZERO.
    for p1 in current_polygon:
        for i in range(len(closest_pa_ids)):
            pa_id = closest_pa_ids[i]
            pa_id = int(pa_id)

            pa_polygon_coords = PA[pa_id]

            if np.any(pa_polygon_coords == p1):
                min_distance = 0.0

            polygon_single_distances = []

            for p2 in pa_polygon_coords:
                d = get_distance(p1, p2)
                polygon_single_distances.append(d)

                min_distance = min(polygon_single_distances)

            shortest_distances.append(min_distance)

    return min(shortest_distances)


@njit(fastmath=True)
def column_split(input, pos1, pos2):
    x1 = pos1
    x2 = pos2 + 1
    cols = x2 - x1
    res = List()
    for i in range(len(input)):
        row = input[i]
        res.append(row[x1:x2])
    return res


@njit(fastmath=True)
def get_bbox(input):
    xcol = column_split(input, 0, 0)
    ycol = column_split(input, 1, 1)

    minx = min(xcol)[0]
    if minx < 278000.0:
        minx = 278000.0

    miny = min(ycol)[0]
    if miny < 5200000.0:
        miny = 5200000.0

    maxx = max(xcol)[0]
    if maxx > 920000.0:
        maxx = 920000.0

    maxy = max(ycol)[0]
    if maxy > 6100000.0:
        maxy = 6100000.0

    if minx < maxx and miny < maxy:
        return List([minx, miny, maxx, maxy])
    else:
        return None


@njit(fastmath=True)
def get_centroid(input):
    cx, cy = np.sum(input, axis=0)
    cx = cx / len(input)
    cy = cy / len(input)
    result = List([cx, cy])
    return result


@njit(fastmath=True)
def get_distance(p1, p2):
    x1, y1 = p1
    x2, y2 = p2

    a = abs(x1 - x2)
    b = abs(y1 - y2)
    c = np.sqrt(a ** 2 + b ** 2)
    return c


@njit(fastmath=True)
def batch_centroids(SECONDARY_DATA):
    result = [get_centroid(SECONDARY_DATA[id]) for id in SECONDARY_DATA.keys()]
    return result


@njit(fastmath=True)
def within_bbox(bounds, point):
    minx, miny, maxx, maxy = bounds

    px, py = point

    if px >= minx and px <= maxx and py >= miny and py <= maxy:
        return 1
    else:
        return 0


@njit(fastmath=True, parallel=True)
def collect_distances(
    id_distances_arr, idd_sample_arr, polygon_ids, SD_centroids, PriDat_centroid
):
    for i in prange(len(polygon_ids)):
        id = polygon_ids[i]
        SD_centroid = SD_centroids[i]

        p2pa_distance = get_distance(SD_centroid, PriDat_centroid)
        data_add_arr = idd_sample_arr.copy()
        data_add_arr[0] = id
        data_add_arr[1] = p2pa_distance
        id_distances_arr[i] = data_add_arr

    return id_distances_arr


@njit(fastmath=True, parallel=True)
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


def convert(geom):
    if type(geom) == Polygon:
        geom = [list(geom.exterior.coords)]

    elif type(geom) == MultiPolygon:
        geom = [list(g.exterior.coords) for g in geom.geoms]

    elif type(geom) == LineString:
        geom = [list(geom.coords)]

    elif type(geom) == MultiLineString:
        geom = [list(g.coords) for g in geom.geoms]

    return geom


def load_geometries_typed_dict(geo_data):
    nested_arr = np.array(
        [
            [0.123, 0.123],
            [0.123, 0.123],
        ]
    ).reshape(2, 2)

    UnpackedDataDict = Dict.empty(key_type=types.int64, value_type=typeof(nested_arr))

    for x in range(len(geo_data)):
        # old_arr = geo_data.iloc[x, geo_data.columns.get_loc("geomarr")][0]
        old_arr = geo_data.iloc[x, geo_data.columns.get_loc("geometry")]
        old_arr = convert(old_arr)
        old_arr = old_arr[0]

        # new_arr = np.empty((len(old_arr), 2)).astype(np.float64)
        new_arr = []

        for i in range(len(old_arr)):
            elem = old_arr[i]
            cx, cy = elem
            new_pair = [cx, cy]
            new_arr.append(new_pair)

        UnpackedDataDict[x] = new_arr

    return UnpackedDataDict


def load_geometries_reg_dict(geo_data):
    UnpackedDataDict = {}

    for x in range(len(geo_data)):
        # old_arr = geo_data.iloc[x, geo_data.columns.get_loc("geomarr")][0]
        old_arr = geo_data.iloc[x, geo_data.columns.get_loc("geometry")]
        old_arr = convert(old_arr)
        old_arr = old_arr[0]

        # new_arr = np.empty((len(old_arr), 2)).astype(np.float64)
        #
        # for i in range(len(old_arr)):
        #     elem = old_arr[i]
        #     cx, cy = elem
        #     new_pair = [cx, cy]
        #     new_arr[i] = new_pair

        UnpackedDataDict[x] = old_arr

    return UnpackedDataDict


def generate_leaves(global_bbox, global_centroid, buffer=1):
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

    # Make a returnable leaves array with all 4 leaves
    # THE ORDER IS SUPER IMPORTANT
    leaves = np.array([p_left_bottom, p_left_top, p_right_top, p_right_bottom]).astype(
        np.float64
    )
    return leaves


@njit
def get_leaf_to_load(TREE_leaves, point):
    for i in range(4):
        leaf_bounds = TREE_leaves[i]
        # leaf_bounds = List(leaf)

        res = within_bbox(leaf_bounds, point)
        if res == 1:
            return i


@njit(fastmath=True)
def collect_ids_in_leaf(DATA, bounds):

    leaf_polygon_ids = List()
    for i in DATA.keys():

        pa_item = DATA[i]

        pa_item_bbox = get_bbox(pa_item)

        minx, miny, maxx, maxy = pa_item_bbox

        p1 = List([minx, miny])
        p2 = List([minx, maxy])
        p3 = List([maxx, maxy])
        p4 = List([maxx, miny])

        bbox = List([p1, p2, p3, p4])

        res = overlaps(bounds, bbox)
        if res == True:
            # Add this bbox's id (aka pointer to the polygon)
            # to the "leaf_contains" so we know which leaf
            # holds which polygons.
            leaf_polygon_ids.append(i)

    return leaf_polygon_ids


@njit
def get_leaf_collection(TREE_leaves, leaf_to_load, DATA):
    """
    Arguments:
    TREE_leaves: holds the leaves we want to check
    DATA: the bboxes we want to check for intersections with the leaves
    leaf_to_load: the leaf we want to check intersections with.

    Returns a list with the polygon ids of the polygons that
    are lying within the leaf.
    """

    bounds = TREE_leaves[leaf_to_load]

    leaf_polygon_ids = collect_ids_in_leaf(DATA, bounds)

    return leaf_polygon_ids


@njit
def destruct_global_coords(DATA_global_coords, all_coordinates):
    for i in prange(len(DATA_global_coords)):
        geom = DATA_global_coords[i]

        for k in range(len(geom)):
            g = geom[k]
            gx = g[0]
            gy = g[1]

            if gx > 278000.0 and gx < 930000.0 and gy > 5200000.0 and gy < 6100000.0:
                all_coordinates.append(g)

    return all_coordinates


def global_polygon_dissolve(DATA):
    DATA_global_coords = List([DATA[key] for key in DATA.keys()])

    all_coordinates = destruct_global_coords(DATA_global_coords, all_coordinates)

    return all_coordinates


def grow_tree(PRIMARY_DATA, SECONDARY_DATA):

    PriDat_global_coords = global_polygon_dissolve(PRIMARY_DATA)
    SD_global_coords = global_polygon_dissolve(SECONDARY_DATA)

    TREE = {
        # - Primary -
        # The main data set that we want to generate information for
        "primary": {
            "globals": {
                "bbox": get_bbox(PriDat_global_coords),
                "centroid": get_centroid(PriDat_global_coords),
            },
            "collections": {0: [], 1: [], 2: [], 3: []},
            "leaves": None,
        },
        # - Secondary -
        # Information we want to combine with the primary data
        "secondary": {
            "globals": {
                "bbox": get_bbox(SD_global_coords),
                "centroid": get_centroid(SD_global_coords),
            },
            "collections": {0: [], 1: [], 2: [], 3: []},
            "leaves": None,
        },
    }

    TREE["primary"]["leaves"] = generate_leaves(
        TREE["primary"]["globals"]["bbox"],
        TREE["primary"]["globals"]["centroid"],
        buffer=1.025,
    )

    TREE["secondary"]["leaves"] = generate_leaves(
        TREE["secondary"]["globals"]["bbox"],
        TREE["secondary"]["globals"]["centroid"],
        buffer=1.025,
    )
    return TREE


def status_bar(current, max_len):

    if sys.stdout.isatty():
        fullwidth = int(os.get_terminal_size().columns)
        width = int(fullwidth - 25)
    else:
        fullwidth = 75
        width = 68
    proc = int(round((current / max_len) * width))
    rest = int(width - proc)

    print(" " * fullwidth)
    sys.stdout.write(style.LINE_UP + style.LINE_CLEAR)
    sys.stdout.write(
        style.LINE_UP
        + style.LINE_CLEAR
        + f">> Working: |"
        + "▉" * proc
        + f"{'-'*rest}| {round((current/ max_len) * 100, 2):03.2f} %"
        + "\r"
    )
    sys.stdout.flush()

    return None
