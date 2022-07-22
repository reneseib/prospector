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


import os

try:
    from pconfig import config

except ImportError:
    Display.import_error()
    os._exit(1)

import time
import sys

for dir in config["init"]["prospector_package_path"]:
    sys.path.append(dir)

import pathlib
import wget
import warnings

warnings.simplefilter(action="ignore", category=UserWarning)
warnings.simplefilter(action="ignore", category=FutureWarning)


import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon, mapping
from pyrosm import OSM, get_data

from multiprocessing import Pool

# Custom imports
from .validate import validate
from .filters import filters
from .stages import stages

from .display.display import Display

from concavehull import ConcaveHull as CH


class Prospector:

    # Constants
    STATE_AREA_KEYS = {"BB": "flaeche", "SN": "AREA_m2"}

    def __init__(self, proj_path, preload=True):
        """
        At instantiation:
        1.  Read the config and store constants in instance, if applicable.

        2.  Load all regularly used files (e.g. protected areas) to RAM so they
            don't have to be read and loaded everytime they are used.

        3.
        """

        self.proj_path = proj_path
        self.main_dir = os.path.join(
            self.proj_path, list(config["directories"].keys())[0]
        )
        self.src_data_dir = os.path.join(self.main_dir, "src_data")
        self.geo_data_dir = os.path.join(self.src_data_dir, "geo_data")

        self.prot_area_path = os.path.join(
            self.main_dir, "src_data", "protected_areas", "gpkg"
        )
        self.results_dir = os.path.join(self.main_dir, "results")

        # Make all functions available by dot notation
        self.validate = validate
        self.filters = filters

        self.all_stages = config["directories"]["prospector_data"]["results"]["stages"]

        # Instance sets itself up based on the config etc.
        self.setup(preload)

    def setup(self, preload):

        # First, check if the dir tree already exists
        counter = 0
        dir_tree_exists = check_dir_tree_from_dict(
            config["directories"], counter, current_dir=self.proj_path
        )

        self.main_dir = os.path.join(
            self.proj_path, list(config["directories"].keys())[0]
        )

        if dir_tree_exists != True:
            build_dir_tree(self.proj_path)

        region_files_exist = check_region_files(self.main_dir)

        # if preload == True:
        #     self.prot_areas = load_prot_areas(self.prot_area_path)

        return None

    def get_regio_file_dir_path(self, regio):
        regio_path = os.path.join(self.geo_data_dir, regio, "osm")
        regio_file_path = os.path.join(regio_path, str(regio + "-latest.osm.pbf"))
        return regio_file_path, regio_path

    ################################################################################
    # staging: the checking wrapper to pass only valid to dos to the passed function
    ################################################################################
    def staging(self, subregions, stage="stage_0", finput=["input"]):
        stage_formal = stage[:1].upper() + stage.replace("_", " ")[1:]
        stage_name = stages.stage_names[stage]

        if subregions != None:

            all_subregions = determine_subregions(subregions)

            stage_done = check_stage_done(self.proj_path, stage_name, all_subregions)

            verify_stage_processing = []
            #
            # if stage_done:
            #     Display.stage_done(stage_formal)
            #
            #     return None
            #
            # else:
            #
            #     for regio in all_subregions:
            #         f = stages.stage_funcs[stage]
            #
            #         stage_finished = f(regio, stage=stage_name)
            #
            #         verify_stage_processing.append(stage_finished)

            # TODO: REVERSE TO NORMAL!!!! JUST FOR TESTING PURPOSES - OVERWRITES PREV DATA

            if not "4" in stage:
                # proc_count = os.cpu_count() - 1
                proc_count = 2
            else:
                proc_count = 4

            with Pool(processes=proc_count) as pool:
                f = stages.stage_funcs[stage]

                multiple_results = [
                    pool.apply_async(f, (regio, stage_name)) for regio in all_subregions
                ]

                verify_stage_processing = [
                    # Set timeout to X hours
                    res.get(timeout=3600 * 24)
                    for res in multiple_results
                ]

            # for regio in all_subregions:
            # f = stages.stage_funcs[stage]
            #
            #     stage_finished = f(regio, stage=stage_name)
            #
            #     verify_stage_processing.append(stage_finished)

            if len(verify_stage_processing) > 0 and all(
                x == True for x in verify_stage_processing
            ):
                Display.stage_finished(stage_formal)

            else:
                Display.stage_proc_error(stage_formal)

        return None


def load_prot_areas(prot_area_path):

    prot_area_files = os.listdir(prot_area_path)
    prot_area_files = [x for x in prot_area_files if x.endswith(".gpkg")]

    prot_areas_loaded = {}

    sys.stdout.write("Preloading 'Protected Areas'...")
    sys.stdout.flush()

    # Load all files as GDF into RAM to make them quickly accessible.
    for file in prot_area_files:
        file_name = file.replace(".gpkg", "")
        file_path = os.path.join(prot_area_path, file)

        prot_areas_loaded[file_name] = gpd.read_file(file_path)

    sys.stdout.write("\033[2K\033[1G")
    sys.stdout.write(
        style.GREEN + "Preloaded all files from 'Protected Areas'.\n" + style.END
    )
    sys.stdout.flush()

    return prot_areas_loaded


"""
START OF UTILS FUNCTIONS
"""
#  Create a custom prgress bar method
def progressBar(current_size, total_size, width):
    sys.stdout.write(f"Downloading: {round(current_size / total_size * 100, 2)}%")
    sys.stdout.flush()
    sys.stdout.write("\033[2K\033[1G")


def download_file(url, location=""):
    wget.download(url, out=location, bar=progressBar)


"""
END OF UTILS FUNCTIONS
"""


def check_dir_tree(proj_path):
    proj_path_exists = os.path.isdir(proj_path)
    if proj_path_exists != True:
        print("Project path does not exist.")
        return False
    else:
        parent_dir = os.path.join(proj_path, "prospector_data")
        parent_dir_exists = os.path.isdir(parent_dir)

        src_data_dir = os.path.join(parent_dir, "src_data")
        src_data_dir_exists = os.path.isdir(src_data_dir)

        results_dir = os.path.join(parent_dir, "results")
        results_dir_exists = os.path.isdir(results_dir)

        if (
            parent_dir_exists != True
            and src_data_dir_exists != True
            and results_dir_exists != True
        ):
            Display.dir_tree_warning()
            return False
        else:
            return True


def build_dir_tree_from_dict(d, current_dir="./"):
    for key, val in d.items():
        dir_path = os.path.join(current_dir, key)
        if os.path.isdir(dir_path) != True:
            os.mkdir(dir_path)

        if type(val) == dict:
            build_dir_tree_from_dict(val, os.path.join(current_dir, key))

        if type(val) == tuple:
            for v in val:
                t_dir_path = os.path.join(current_dir, key, v)
                if os.path.isdir(t_dir_path) != True:
                    os.mkdir(os.path.join(current_dir, key, v))


def check_dir_tree_from_dict(d, counter, current_dir="./"):
    dir_checks = []
    i = 0
    for key, val in d.items():
        dir_path = os.path.join(current_dir, key)
        i += 1
        if os.path.isdir(dir_path) == True:
            dir_checks.append(True)
        else:
            dir_checks.append(False)

        if type(val) == dict:
            check_dir_tree_from_dict(val, counter, os.path.join(current_dir, key))

        if type(val) == tuple:
            for v in val:
                i += 1
                t_dir_path = os.path.join(current_dir, key, v)
                if os.path.isdir(t_dir_path) == True:
                    dir_checks.append(True)
                else:
                    dir_checks.append(False)

    if all(x == True for x in dir_checks) == True:
        return True
    else:
        return False


def build_dir_tree(proj_path):
    proj_path_exists = os.path.isdir(proj_path)
    if proj_path_exists != True:
        print("Error: Project path does not exist.")
        return False
    else:
        directories = config["directories"]

        build_dir_tree_from_dict(directories, current_dir=proj_path)

        Display.dir_tree_build_success()
        return True


def check_region_files(main_dir):
    geo_data_dir = os.path.join(main_dir, "src_data", "geo_data")

    req_subregions = config["area"]["subregions"]
    check_all_req_subregions_exist = all(
        elem in os.listdir(geo_data_dir) for elem in req_subregions
    )

    output_file_checks = []

    if check_all_req_subregions_exist != False:
        for subregion in req_subregions:
            subregion_path = os.path.join(geo_data_dir, subregion)
            osm_subregion_path = os.path.join(subregion_path, "osm")

            output_file_name = f"{subregion}-latest.osm.pbf"
            output_file_path = os.path.join(osm_subregion_path, output_file_name)

            # Need to replace the underscore with dash for geofabrik conformity
            osm_file = f"{subregion}-latest.osm.pbf".replace("_", "-")

            output_file_exists = os.path.isfile(output_file_path)
            output_file_checks.append(output_file_exists)
            if output_file_exists != True:
                print(
                    f"OSM file '{osm_file}' missing.\nWill download the latest one to {osm_subregion_path}"
                )
                download_url = f"""
                https://download.geofabrik.de/{config["area"]["continent"]}/{config["area"]["country"]}/{osm_file}
                """
                download_url = download_url.strip().lower()

                try:
                    download_file(download_url, location=output_file_path)
                    print("Download finished successfully\n")
                except:
                    print(download_url)
                    raise

        if all(x == True for x in output_file_checks):
            return True


def determine_subregions(subregions):
    if len(subregions) > 0:
        all_subregions = subregions

    elif len(subregions) == 0:
        all_subregions = config["area"]["subregions"]
    return all_subregions


def check_stage_done(proj_path, stage_name, all_subregions):
    stage_done = [
        os.path.isfile(
            os.path.join(
                # Build results directory
                os.path.join(proj_path, "prospector_data", "results"),
                "stages",
                stage_name,
                regio,
                "gpkg",
                "".join([regio, "-", stage_name, ".gpkg"]),
            )
        )
        for regio in all_subregions
    ]

    if all(x == True for x in stage_done):
        return True
    else:
        return False
