#!/usr/bin/python3

from pconfig import config

import sys
import os
import geopandas as gpd
from typing import Union, NewType
import numpy as np
import numba as nb
from numba import njit, typeof, prange
from numba import errors
from numba.typed import List as typedList
from shapely.geometry import Point as shapePoint
from shapely.geometry import Polygon as shapePolygon
from shapely.geometry import MultiPolygon as shapeMultiPolygon
from pyproj import Transformer
from geopandas.geoseries import GeoSeries
from pandas.core.series import Series
from ast import literal_eval
import re
import json

# Custom imports
from pconfig import config

for dir in config["init"]["prospector_package_path"]:
    sys.path.append(dir)

for dir in config["init"]["proj_path"]:
    if os.path.isdir(dir):
        proj_path = dir


main_dir = os.path.join(proj_path, list(config["directories"].keys())[0])
src_data_dir = os.path.join(main_dir, "src_data")
geo_data_dir = os.path.join(src_data_dir, "geo_data")
results_dir = os.path.join(main_dir, "results")


def progressbar(count, total, status=""):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = "=" * filled_len + "-" * (bar_len - filled_len)

    sys.stdout.write("[%s] %s%s ...%s\r" % (bar, percents, "%", status))
    sys.stdout.flush()


def get_last_stage_name(current_stage):
    all_stages = tuple(
        list(config["directories"]["prospector_data"]["results"]["stages"].keys())
    )
    current_idx = all_stages.index(current_stage)
    prev_idx = current_idx - 1
    return all_stages[prev_idx]


def load_prev_stage_to_gdf(regio, current_stage):
    # Get name of previous stage
    prev_stage = get_last_stage_name(current_stage)

    # Build path to the regio's last stage gpkg file
    last_stage_data = os.path.join(
        results_dir, "stages", prev_stage, regio, "gpkg", f"{regio}-{prev_stage}.gpkg"
    )

    # 2. Load file to GDF
    if os.path.isfile(last_stage_data):
        gdf = gpd.read_file(last_stage_data)
        gdf = gpd.GeoDataFrame(gdf)
        for col in gdf.columns:
            if col.startswith("np_"):
                gdf[col] = gdf[col].apply(lambda x: arrify(x))

        return gdf

    else:
        return gpd.GeoDataFrame([])


def save_current_stage_to_file(gdf, regio, current_stage):
    output_file_gpkg = os.path.join(
        results_dir,
        "stages",
        current_stage,
        regio,
        "gpkg",
        f"{regio}-{current_stage}.gpkg",
    )

    # Before saving, we need to convert all np.arrays to strings
    for col in gdf.columns:
        if col.startswith("np_"):
            gdf[col] = gdf[col].apply(lambda x: stringify(x))

    try:
        gdf.to_file(output_file_gpkg, driver="GPKG")
        return True

    except Exception as e:
        print(e)
        return False


def load_file_to_gdf(file_path, src_crs=4326, target_crs=25832):
    f = gpd.read_file(file_path)
    if src_crs is not None:
        gdf = gpd.GeoDataFrame(f).set_crs(src_crs)
    if target_crs is not None:
        gdf = gpd.GeoDataFrame(f).to_crs(src_crs)

    return gdf


def flatten_polygon(x):
    if isinstance(x, list):
        return [a for i in x for a in flatten_polygon(i)]
    elif isinstance(x, tuple):
        if len(x) == 2 and type(x[0]) == float:
            return [x]
        else:
            return [a for i in x for a in flatten_polygon(i)]


###############################################################################
#                          Custom Type Definitions                            #
###############################################################################
Point = NewType("Point", np.array([1.0, 2.0]).astype(np.float64))

AoA = NewType(
    "AoA", np.array([[1.0, 2.0], [1.0, 2.0]]).astype(np.float64)
)  # Array of arrays

Polygon = NewType("Polygon", AoA)

LoA = NewType(
    "LoA", list([np.array([1.0, 2.0, 3.0]).astype(np.float64)])
)  # LoA = List of Arrays

MultiPolygon = NewType("MultiPolygon", LoA)

Geometry = Union[Point, Polygon, MultiPolygon]

TrueGeometry = Union[shapePoint, shapePolygon, shapeMultiPolygon]


###############################################################################
#                                Functions                                    #
###############################################################################


def isPoint(geom: Geometry):
    if (
        isinstance(geom, np.ndarray)
        and len(geom) >= 2
        and isinstance(geom[0], np.float64)
    ):
        return True

    return False


def isPolygon(geom: Geometry):
    if (
        isinstance(geom, np.ndarray)
        and len(geom) >= 2
        and isinstance(geom[0], np.ndarray)
    ):
        return True
    return False


def isMultiPolygon(geom: Geometry):
    if isinstance(geom, list) and isinstance(geom[0], np.ndarray):
        return True
    return False


def geom2arr(geom: TrueGeometry, reverse_coords=False):
    if isinstance(geom, shapePoint):
        if reverse_coords == True:
            arr = np.array([x[0] for x in np.flip(geom.coords.xy)]).astype(np.float64)
        else:
            arr = np.array([x[0] for x in geom.coords.xy]).astype(np.float64)

    if isinstance(geom, shapePolygon):
        if reverse_coords == True:
            arr = np.array([np.flip(x) for x in geom.exterior.coords]).astype(
                np.float64
            )
        else:
            arr = np.array([x for x in geom.exterior.coords]).astype(np.float64)

    if isinstance(geom, shapeMultiPolygon):
        arr = list([np.array(g.exterior.coords).astype(np.float64) for g in geom.geoms])

    return arr


def crs_transform(geom: Geometry, src: int, target: int) -> Geometry:
    """
    Allows to transform coordinates stored in arrays
    """
    crs_transformer = Transformer.from_crs(f"epsg:{src}", f"epsg:{target}")

    # Transformn Point
    if isPoint(geom):
        x_old, y_old = geom
        x_new, y_new = crs_transformer.transform(x_old, y_old)
        geom[0] = x_new
        geom[1] = y_new

    # Transform Polygon
    elif isPolygon(geom):
        for i in range(len(geom)):
            x_old, y_old = geom[i]
            transformed_coords = crs_transformer.transform(x_old, y_old)
            geom[i] = np.array(transformed_coords).astype(np.float64)

    # Transform MultiPolygon
    elif isMultiPolygon(geom):
        # Iterate over all polygons
        for i in range(len(geom)):
            polygon = geom[i]
            # Iterate over all points
            for k in range(len(polygon)):
                x_old, y_old = polygon[k]
                # Transform point coords
                transformed_coords = crs_transformer.transform(x_old, y_old)
                # Overwrite old with new coords
                polygon[k][0] = transformed_coords[0]
                polygon[k][1] = transformed_coords[1]
            # Overwrite old with new polygon with transformed coords
            geom[i] = polygon

    return geom


def series2list(series: Series):
    """
    Creates a list with the content of a series
    """
    # Type-check input
    # if type(series) != Series:
    if not isinstance(series, Series):
        errors.TypingError("series2list: Argument must be of type pandas.Series")

    # Type-check all elements of series if they are arrays
    # check_all_arr_series = series.apply(lambda x: isinstance(x, np.array))
    if False in [isinstance(x, np.ndarray) for x in series]:
        errors.TypingError("series2list: All elements of series must be arrays")

    # Create list to store all np.arrays of series
    series_list = []

    # If all elements are np.arrays, append all elemts to list
    series.apply(lambda x: series_list.append(x))
    return series_list


def pointcloud(geom: Polygon) -> np.array:
    """
    Returns a list of points from a series of geometries as np.array
    """
    # Type check input
    if isinstance(geom, np.ndarray):
        """
        This case applies when we use pointcloud on a series
        and get a regular polygon as input
        """
        return geom

    # Type check input
    elif isinstance(geom, list):
        """
        This case applies when we get a multipolygon as input or if we apply
        pointcloud on a list (e.g. a series converted to a list) with regular
        or multipolygons
        """
        # Cloud-list in which points are stored
        cloud = []

        # Type check if the elements of 'geom' are lists as well
        # which is an indication that it is a multipolygon so
        # we know that we need to loop at one level deeper
        if any([isinstance(geo, list) for geo in geom]):
            # Iterate over the elements of the list
            for geo in geom:
                if isinstance(geo, list):
                    # Iterate over points of geometry & store them in list
                    [
                        [cloud.append(np.array(p).astype(np.float64)) for p in g]
                        for g in geo
                    ]
                else:
                    [cloud.append(np.array(g).astype(np.float64)) for g in geo]

        else:
            # No lists in 'geom', so it's a regular polygon

            # Cloud-list in which points are stored
            cloud = []

            # Iterate over all points and store them in the list
            [[cloud.append(np.array(p).astype(np.float64)) for p in g] for g in geom]

        # Convert cloud to np.array
        cloud = np.array(cloud).astype(np.float64)
        return cloud

    else:
        errors.TypingError("point_cloud_polygon: input was not a list.")


def column_split(geom: AoA, index: int):
    """
    Creates a column/len(1)-slice of an array
    """
    index1 = index + 1
    column = np.empty((len(geom))).astype(np.float64)

    # Iterate over each row/array
    for i in range(len(geom)):

        # Slice out the desired index
        row = geom[i]

        # Store the slice in empty array
        column[i] = row[index:index1][0]

    return column


def get_centroid(point_cloud: AoA) -> np.array:
    """
    Returns the centroid as list, the mean of all the points
    """
    # Get all x and y coordinates as single arrays
    xcol = column_split(point_cloud, 0)
    ycol = column_split(point_cloud, 1)

    # Calculate mean of x and y coordinates
    x = np.sum(xcol) / len(xcol)
    y = np.sum(ycol) / len(ycol)

    return np.array([x, y]).astype(np.float64)


def get_bbox(point_cloud: Geometry) -> np.array:
    """
    Returns the bounding box of a geometry or point_cloud
    """
    # Get all x and y coordinates as single arrays
    xcol = column_split(point_cloud, 0)
    ycol = column_split(point_cloud, 1)

    # Calculate min and max of all x and y coordinates
    minx = min(xcol)
    miny = min(ycol)
    maxx = max(xcol)
    maxy = max(ycol)

    return np.array([minx, miny, maxx, maxy]).astype(np.float64)


@njit
def overlaps(geom: Geometry, bbox: Geometry) -> bool:
    """
    Returns True if the geometry overlaps with the bbox
    """
    minx, miny, maxx, maxy = bbox
    # Iterate over all points in the geometry array
    for point in geom:
        x, y = point

        # Check if the coordinates of the point lie within the bbox
        if x >= minx and x <= maxx and y >= miny and y <= maxy:
            return True

    return False


def get_extrema(geom: Geometry) -> np.array:
    """
    Returns the extreme points of a geometry: the point with the minx
    coordinate, the point with maxx coordinate, the point with miny coordinate
    and the point with maxy coordinate
    """
    minx, miny, maxx, maxy = get_bbox(geom)

    extrema = np.empty((4, 2)).astype(np.float64)

    for point in geom:
        if point[1] == miny:
            extrema[0] = np.array(point).astype(np.float64)
        if point[0] == minx:
            extrema[1] = np.array(point).astype(np.float64)
        if point[1] == maxy:
            extrema[2] = np.array(point).astype(np.float64)
        if point[0] == maxx:
            extrema[3] = np.array(point).astype(np.float64)

    return extrema


def stringify(arr: Union[np.ndarray, list]) -> str:
    if type(arr) == np.ndarray or type(arr) == list:
        arrstr = str(arr).strip()
        arrstr = re.sub(r"(\d)(\s|\s\s+)(\d)", r"\1, \3", arrstr)
        arrstr = arrstr.split("\n")
        arrstr = [x.strip() for x in arrstr]
        arrstr = ",".join(arrstr)
        arrstr = re.sub(r"\s\s+", " ", arrstr)
        arrstr = arrstr.replace(". ", ".0000001, ")
        arrstr = arrstr.replace(",,", ",")
        arrstr = arrstr.replace(" ", "")
        if arrstr.startswith(","):
            arrstr = arr[1:]
        if arrstr.endswith(","):
            arrstr = arr[:-1]

        arrstr = arrstr.replace("array(", "").replace(")", "")
        arrstr = arrstr.replace("list(", "").replace(")", "")
        arrstr = arrstr.replace("Ellipsis", "")
        arrstr = arrstr.replace(",,", ",").replace("][", "],[")
    else:
        arrstr = arr
    return arrstr


def arrify(input_arrstr: str) -> np.ndarray:
    # input_arrstr = input_arrstr.replace("Ellipsis", "")
    # input_arrstr = input_arrstr.replace("list(", "").replace(")", "")

    # Enable loading of malformed input
    re_malformation = re.compile(r"(\d)\-(\d)")
    input_arrstr = re.sub(re_malformation, r"\1, -\2", input_arrstr)

    try:
        if input_arrstr.startswith("[array"):
            # It is a multipolygon wrapped in a list
            arrstr = input_arrstr[1:-1]
            arrstr = arrstr.split("#")
            arr = [np.array(literal_eval(x.strip())) for x in arrstr]
        else:
            arr = np.array(literal_eval(input_arrstr))

        return arr
    except:
        print("----------")
        print(input_arrstr)
        arrstr = input_arrstr[1:-1]
        print(arrstr)
        arrstr = arrstr.split("#")
        print(arrstr)
        print("----------")
        raise


def get_distance(p1, p2):
    """
    Returns the distance between two points
    """
    if len(p1) == 2 and len(p2) == 2:
        x1, y1 = p1
        x2, y2 = p2

        a = abs(x1 - x2)
        b = abs(y1 - y2)
        c = np.sqrt(a ** 2 + b ** 2)
        return c

    else:
        errors.TypingError("get_distance: Arguments have to be two points as np.arrays")
