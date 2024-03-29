import os, sys

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

# Stage 0 Imports
from pyrosm import OSM, get_data
import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon
from pyrosm import OSM, get_data
import multiprocessing

# Custom imports
from pconfig import config

for dir in config["init"]["prospector_package_path"]:
    sys.path.append(dir)

from concavehull import ConcaveHull as CH

# Set up dir vars
for dir in config["init"]["proj_path"]:
    if os.path.isdir(dir):
        proj_path = dir

main_dir = os.path.join(proj_path, list(config["directories"].keys())[0])
src_data_dir = os.path.join(main_dir, "src_data")
geo_data_dir = os.path.join(src_data_dir, "geo_data")
results_dir = os.path.join(main_dir, "results")


def f_stage_0(regio, stage="0-filtered_by_landuse"):
    # Internal functions
    def get_regio_file_dir_path(regio):
        regio_path = os.path.join(geo_data_dir, regio, "osm")
        regio_file_path = os.path.join(regio_path, str(regio + "-latest.osm.pbf"))
        return regio_file_path, regio_path

    def filter_osm_by_landuse(
        gdf, filter_landuse, nodes=False, ways=False, relations=True
    ):
        wrk_gdf = gdf.get_data_by_custom_criteria(
            custom_filter={"landuse": [filter_landuse]},
            # Keep data matching the criteria above
            filter_type="keep",
            keep_nodes=nodes,
            keep_ways=ways,
            keep_relations=relations,
        )
        return wrk_gdf

    # Main function
    regio_file_path, regio_path = get_regio_file_dir_path(regio)

    output_path_gpkg = os.path.join(
        results_dir,
        "stages",
        "0-filtered_by_landuse",
        regio,
        "gpkg",
    )

    if os.path.isdir(output_path_gpkg) != True:
        pathlib.Path(output_path_gpkg).mkdir(parents=True, exist_ok=True)

    output_file_gpkg = os.path.join(
        output_path_gpkg, f"{regio}-0-filtered_by_landuse.gpkg"
    )

    if os.path.isfile(output_file_gpkg) != True:
        print(f"Starting filtering for 'farmland' in '{regio}' now.")
        # Load OSM data from file
        gdf = OSM(regio_file_path)

        # Filter the OSM data with custom filter
        wrk_gdf = filter_osm_by_landuse(
            gdf,
            "farmland",
            nodes=False,
            ways=True,
            relations=True,
        )

        # Since data is loaded from OSM, the CRS is always 4326!
        src_crs = 4326

        # Target CRS depends on the state:
        # Western Germany's states are 25832, Eastern 25833
        target_crs = config["epsg"][regio]

        # Apply the CRS to the GDF
        gdf = gpd.GeoDataFrame(wrk_gdf).set_crs(src_crs).to_crs(target_crs)

        try:
            gdf.to_file(output_file_gpkg, driver="GPKG")
            print(f"Successfully filtered and saved results to\n{output_file_gpkg}\n")
            return True
        except Exception as e:
            print("\n")
            raise
            print("\n")
            os._exit(1)
            return False
    else:
        print(f"{output_file_gpkg}: File already exists. Skipping this conversion.")
        return False
