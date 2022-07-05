import random
from shapely.geometry import Point, Polygon, MultiPolygon, mapping
from shapely import wkt
import numpy as np
import geopandas as gpd
import os
import sys
from math import radians, cos, sin, asin, sqrt
from numba import njit
