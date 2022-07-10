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
