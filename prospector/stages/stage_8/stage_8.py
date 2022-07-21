import os, sys

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Stage 0 Imports
import geopandas as gpd
import numpy as np
import json

# Custom imports
from pconfig import config
from proxpy.proxpy import ProxyRequest

for dir in config["init"]["prospector_package_path"]:
    sys.path.append(dir)

from util import util


for dir in config["init"]["proj_path"]:
    if os.path.isdir(dir):
        proj_path = dir

main_dir = os.path.join(proj_path, list(config["directories"].keys())[0])
src_data_dir = os.path.join(main_dir, "src_data")
geo_data_dir = os.path.join(src_data_dir, "geo_data")
results_dir = os.path.join(main_dir, "results")


def add_solar_data(i, proxy_server: ProxyRequest, lat: float, lon: float) -> bool:
    """
    Get request to solaratlas to fetch data, returns json
    """
    api_url = f"https://api.globalsolaratlas.info/data/lta?loc={lat},{lon}"

    response = proxy_server.get(api_url)
    if response == 200:
        data = json.loads(response.content)["annual"]["data"]
        """
        Returns a dict like this:
        {'PVOUT_csi': 1036.3988037109375, 'DNI': 905.390625, 'GHI': 1055.328125, 'DIF': 565.359375, 'GTI_opta': 1226.7890625, 'OPTA': 36, 'TEMP': 8.5625, 'ELE': 331}
        """
        gdf[i, "solar_PVOUT_csi"] = data["PVOUT_csi"]
        gdf[i, "solar_DNI"] = data["DNI"]
        gdf[i, "solar_GHI"] = data["GHI"]
        gdf[i, "solar_DIF"] = data["DIF"]
        gdf[i, "solar_GTI_opta"] = data["GIT_opta"]
        gdf[i, "solar_OPTA"] = data["OPTA"]
        gdf[i, "solar_TEMP"] = data["TEMP"]

        return True
    else:
        return False


def f_stage_8(regio, stage="8-added_solar_data"):
    """
    Stage 8:
    Adds lots of solar data of the area
    """

    print(regio)

    # Load previous stage data
    gdf = util.load_prev_stage_to_gdf(regio, stage)

    # Set CRS to local one and transform to 4326
    gdf = gdf.set_crs(config["epsg"][regio], allow_override=True).to_crs(4326)

    # Get centroid of each geometry, switch coords and convert to tuple
    gdf["centroid"] = gdf["geometry"].centroid
    gdf["centroid"] = gdf["centroid"].apply(
        lambda x: tuple([x.coords.xy[1][0], x.coords.xy[0][0]])
    )

    # Set up new columns for the solar data
    gdf["solar_PVOUT_csi"] = None
    gdf["solar_DNI"] = None
    gdf["solar_GHI"] = None
    gdf["solar_DIF"] = None
    gdf["solar_GTI_opta"] = None
    gdf["solar_OPTA"] = None
    gdf["solar_TEMP"] = None

    # Create proxyserver instance to use in loop
    proxy_server = ProxyRequest()

    if len(gdf) > 0:

        # Iterage over gdf
        for i in range(len(gdf)):
            # Get centroid coordinates
            lat, lon = gdf.loc[i, "centroid"]
            added = add_solar_data(i, proxy_server, lat, lon)
            if added == True:
                pass
            else:
                break
                os._exit(3)

        # Save the final result
        final_save = util.save_current_stage_to_file(gdf, regio, stage)
        print(f"{regio}: all solar data saved")

        if final_save == True:
            return True
        else:
            return False

    return None
