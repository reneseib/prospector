import os, sys

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Stage 0 Imports
import geopandas as gpd
import numpy as np
import json
import random
import time
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem

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


def build_bbox(lon, lat):
    lonMin = lon
    latMin = lat

    lonMax = lonMin * 1.00001
    latMax = latMin * 1.0000001

    BBOX = f"{lonMin},{latMin},{lonMax},{latMax}"
    return BBOX


def add_geoportal_data(
    gdf, i, proxy_server: ProxyRequest, throttle: float, lat: float, lon: float
) -> bool:
    """
    Get request to solaratlas to fetch data, returns json
    """

    # Error handling for the request is implemented in the
    # proxy_request.get method so it returns a status code 403
    # if an error happens. In this case we just return false since
    # the cells in GDF are already NaN.

    BBOX = build_bbox(coordinates)

    api_url = f"""https://www.geoportal.de/openurl/https/services.bgr.de/wms/boden/sqr1000/?SERVICE=WMS&\
    VERSION=1.3.0&\
    REQUEST=GetFeatureInfo&\
    FORMAT=image/png&\
    TRANSPARENT=true&\
    QUERY_LAYERS=32&\
    CACHEID=3167993&\
    LAYERS=32&\
    SINGLETILE=false&\
    WIDTH=16&\
    HEIGHT=16&\
    INFO_FORMAT=text%2Fxml&\
    FEATURE_COUNT=1&\
    I=8&\
    J=8&\
    CRS=EPSG:25832&\
    STYLES=&\
    BBOX={BBOX}"""

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Cookie": "geoportal_matomo_consent=0; TS0138f14a=01a5115d43990e409c7efb4273445b089dc2291674dbef9861fa37988948d00517be1b6474aaf266cff77218188f89f742eb935736",
        "Host": "www.geoportal.de",
        "Referer": "https://www.geoportal.de/map.html?map=tk_02-boeden-bewertung",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Sec-GPC": "1",
        "User-Agent": user_agent_rotator.get_random_user_agent(),
    }

    response = proxy_server.get(api_url, headers=headers, throttle=throttle)

    """
    Returns xml like this:

    <FeatureInfoResponse xmlns:esri_wms="http://www.esri.com/wms" xmlns="http://www.esri.com/wms">
    <FIELDS PixelValue="NoData"></FIELDS>
    </FeatureInfoResponse>

    <FeatureInfoResponse xmlns:esri_wms="http://www.esri.com/wms" xmlns="http://www.esri.com/wms">
    <FIELDS Classvalue="5" PixelValue="94.500000"></FIELDS>
    </FeatureInfoResponse>
    """

    if response.status_code == 200:
        re_pv = re.compile(r'PixelValue="(.*?)"')
        pixel_value = re.findall(re_pv, response)

        soil_score = 0

        if pixel_value and len(pixel_value) != 0:
            if "nodata" not in str(pixel_value).lower():
                soil_score = float(pixel_value[0].replace(",", "."))
            else:
                soil_score = 0

        return soil_score
    else:
        return False


def f_stage_8(regio, stage="8_5-added_geportal_data"):
    """
    Stage 8.5:
    Adds several data from https://geoportal.de
    - soil score

    """

    # if not os.path.isfile(
    #     f"/common/ecap/prospector_data/results/stages/8-added_solar_data/{regio}/gpkg/{regio}-8-added_solar_data.gpkg"
    # ):
    if 1 == 1:
        # Load previous stage data
        gdf = util.load_prev_stage_to_gdf(regio, stage)
        if len(gdf) > 0:
            print(regio)

            t0 = time.time()

            # Get centroid of each geometry, switch coords and convert to tuple
            gdf["centroid"] = gdf["geometry"].centroid
            gdf["centroid"] = gdf["centroid"].apply(
                lambda x: tuple([x.coords.xy[0][0], x.coords.xy[1][0]])
            )

            # Set up new column for the soil score
            gdf["soil_score"] = None

            # Create proxyserver instance to use in loop
            proxy_server = ProxyRequest()

            if len(gdf) > 0:

                gdf["soil_score"] = gdf["centroid"].apply(lambda x: add_geoportal_data)

                # Iterage over gdf
                for i in range(len(gdf)):
                    # Get centroid coordinates
                    lat, lon = gdf.loc[i, "centroid"]

                    throttle = 0.45  # in seconds
                    added = add_geoportal_data(gdf, i, proxy_server, throttle, lat, lon)

                gdf = gdf.drop(columns=["centroid"])

                # Save the final result
                final_save = util.save_current_stage_to_file(gdf, regio, stage)
                print(f"{regio}: all solar data saved")

                if final_save == True:
                    return True
                else:
                    return False
    else:
        print(f"For {regio} solar data already exist")
        return False
