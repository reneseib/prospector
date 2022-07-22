import os, sys

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


# Stage 0 Imports
from pyrosm import OSM, get_data
import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon, mapping
from pyrosm import OSM, get_data
import numpy as np

# Custom imports
from pconfig import config

for dir in config["init"]["prospector_package_path"]:
    sys.path.append(dir)

# Import Stages functions
from stage_0.stage_0 import f_stage_0
from stage_0_5.stage_0_5 import f_stage_0_5
from stage_1.stage_1 import f_stage_1
from stage_2.stage_2 import f_stage_2
from stage_3.stage_3 import f_stage_3
from stage_4.stage_4 import f_stage_4
from stage_5.stage_5 import f_stage_5
from stage_6.stage_6 import f_stage_6
from stage_6_5.stage_6_5 import f_stage_6_5
from stage_7.stage_7 import f_stage_7
from stage_8.stage_8 import f_stage_8


stage_funcs = {
    "stage_0": f_stage_0,
    "stage_0_5": f_stage_0_5,
    "stage_1": f_stage_1,
    "stage_2": f_stage_2,
    "stage_3": f_stage_3,
    "stage_4": f_stage_4,
    "stage_5": f_stage_5,
    "stage_6": f_stage_6,
    "stage_6_5": f_stage_6_5,
    "stage_7": f_stage_7,
    "stage_8": f_stage_8,
}

stage_names = {
    "stage_0": "0-filtered_by_landuse",
    "stage_0_5": "0_5-convert-multipolygons-to-polygon",
    "stage_1": "1-filtered_by_size",
    "stage_2": "2-added_centroids",
    "stage_3": "3-filtered_by_intersection_protected_area",
    "stage_4": "4-added_nearest_protected_area",
    "stage_5": "5-added_nearest_substation",
    "stage_6": "6-added_slope",
    "stage_6_5": "6_5-added_slope_results",
    "stage_7": "7-added_nearest_agrargen",
    "stage_8": "8-added_solar_data",
}
