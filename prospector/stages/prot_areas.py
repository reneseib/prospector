import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

import multiprocessing
from pyrosm import OSM, get_data
import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon, mapping
from shapely import wkt
from pyrosm import OSM, get_data
import numpy as np
from pyproj import Geod

# Custom imports
from pconfig import config

for dir in config["init"]["prospector_package_path"]:
    sys.path.append(dir)

from util import util


for dir in config["init"]["proj_path"]:
    if os.path.isdir(dir):
        proj_path = dir


print("Preloading 'Protected Areas'...")

main_dir = os.path.join(proj_path, list(config["directories"].keys())[0])
src_data_dir = os.path.join(main_dir, "src_data")
geo_data_dir = os.path.join(src_data_dir, "geo_data")
results_dir = os.path.join(main_dir, "results")


prot_area_dir_path = os.path.join(src_data_dir, "protected_areas", "gpkg")
prot_area_files = os.listdir(prot_area_dir_path)

prot_area_file_paths = [
    os.path.join(src_data_dir, "protected_areas", "gpkg", file_name)
    for file_name in prot_area_files
    if file_name.endswith(".gpkg")
]

prot_area_names = [
    "lsg_gesamt_de",
    "nsg_gesamt_de",
    "biosphaeren_gesamt_de",
    "fauna_flora_habitat_gesamt_de",
    "nationalparks_gesamt_de",
    "naturmonumente_gesamt_de",
    "vogelschutz_gesamt_de",
    "naturparks_gesamt_de",  # unimportant
]

# Load prot area files to GDF and store the GDFs in a dict
# GET THE CORRECT CRS AND PROJECT TO IT TO FIND THE INTERSECTIONS
# SINCE PROT AREA MAPS ARE ALL IN CRS=25832!!!
prot_area_data_list = [
    {
        name: util.load_file_to_gdf(file_path, src_crs=25832)
        for name in prot_area_names
        if name in file_path
    }
    for file_path in prot_area_file_paths
]

# Unpack list of dicts to a single dict
# Eventually the prot area data are stored as GDFs
# in a dict
prot_area_data = {}
for pair in prot_area_data_list:
    prot_area_data[list(pair.keys())[0].replace("_gesamt_de", "")] = list(
        pair.values()
    )[0]


# Create empty lists for results
lsg_overlap_list = []
nsg_overlap_list = []
biosphaere_overlap_list = []
fauna_flora_overlap_list = []
nationalparks_overlap_list = []
naturmonumente_overlap_list = []
vogelschutz_overlap_list = []

overlap_data = {
    "lsg_overlap": {
        "list": lsg_overlap_list,
        "data": prot_area_data["lsg"],
    },
    "nsg_overlap": {
        "list": nsg_overlap_list,
        "data": prot_area_data["nsg"],
    },
    "biosphaere_overlap": {
        "list": biosphaere_overlap_list,
        "data": prot_area_data["biosphaeren"],
    },
    "fauna_flora_overlap": {
        "list": fauna_flora_overlap_list,
        "data": prot_area_data["fauna_flora_habitat"],
    },
    "nationalparks_overlap": {
        "list": nationalparks_overlap_list,
        "data": prot_area_data["nationalparks"],
    },
    "naturmonumente_overlap": {
        "list": naturmonumente_overlap_list,
        "data": prot_area_data["naturmonumente"],
    },
    "vogelschutz_overlap": {
        "list": vogelschutz_overlap_list,
        "data": prot_area_data["vogelschutz"],
    },
}
