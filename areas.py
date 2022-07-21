import os, sys

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
# Stage 0 Imports
from pyrosm import OSM, get_data
import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon
from multiprocessing import Pool

# Custom imports
from pconfig import config

for dir in config["init"]["prospector_package_path"]:
    sys.path.append(dir)


# Set up dir vars
for dir in config["init"]["proj_path"]:
    if os.path.isdir(dir):
        proj_path = dir


regios = [
    "baden_wuerttemberg",
    "bayern",
    "brandenburg",
    "berlin",
    "bremen",
    "hamburg",
    "hessen",
    "mecklenburg_vorpommern",
    "niedersachsen",
    "nordrhein_westfalen",
    "rheinland_pfalz",
    "saarland",
    "sachsen_anhalt",
    "sachsen",
    "schleswig_holstein",
    "thueringen",
]


def filter_osm_by_powersrc(
    osm_buff, filter_powersrc, nodes=False, ways=False, relations=True
):
    wrk_buff = osm_buff.get_data_by_custom_criteria(
        custom_filter={"power": ["plant"], "plant:source": [filter_powersrc]},
        # Keep data matching the criteria above
        filter_type="keep",
        keep_nodes=nodes,
        keep_ways=ways,
        keep_relations=relations,
    )
    return wrk_buff


def save_powersrc(regio, powersrc, gdf):
    output_path_gpkg = os.path.join(trgt_dir, powersrc, regio)

    output_file_gpkg = os.path.join(output_path_gpkg, f"{regio}-{powersrc}_power.gpkg")

    if os.path.isfile(output_file_gpkg) != True:
        print(f"Starting: {powersrc} in {regio}")

        # Filter the OSM data with custom filter
        work_buff = filter_osm_by_powersrc(
            gdf,
            powersrc,
            nodes=False,
            ways=True,
            relations=True,
        )

        # Since data is loaded from OSM, the CRS is always 4326!
        src_crs = 4326

        # Target CRS depends on the state:
        # Western Germany's states are 25832, Eastern 25833
        target_crs = config["epsg"][regio]

        # Set the CRS to the GDF
        gdf = gpd.GeoDataFrame(work_buff).set_crs(src_crs).to_crs(target_crs)

        try:
            gdf.to_file(output_file_gpkg, driver="GPKG")
            print(
                f"Successfully filtered and saved results for {powersrc} in {regio}\n\n"
            )
            return True
        except Exception as e:
            print("\n")
            raise
            print("\n")
            os._exit(3)
            return False
    else:
        print(f"Skipping {powersrc} in {regio} - already done!")


trgt_dir = "/common/ecap/prospector_data/src_data/areas"

powersrcs = ["solar", "wind", "hydro"]


for regio in regios:

    regio_osm_file = f"/common/ecap/prospector_data/src_data/geo_data/{regio}/osm/{regio}-latest.osm.pbf"

    gdf = OSM(regio_osm_file)

    for powersrc in powersrcs:
        bla = save_powersrc(regio, powersrc, gdf)
    gdf = None
