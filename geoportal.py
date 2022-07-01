import requests
from pyproj import Proj, transform
import re
import random
import time

point = (50.81129535376496, 13.291240115448738)

# COMMON VARIABLES
s = random.uniform(0,2)


def reform_coordinates(coordinates):
    (x, y) = coordinates
    inProj  = Proj('EPSG:25832')
    outProj = Proj('EPSG:4326')
    lat, lon = transform(inProj,outProj,x,y)
    return (lat, lon)


def transform_coordinates(coordinates):
    (x, y) = coordinates
    inProj  = Proj('EPSG:4326')
    outProj = Proj('EPSG:25832')
    lat, lon = transform(inProj,outProj,x,y)
    return (lat, lon)


def build_bbox(coordinates):
    lon, lat = transform_coordinates(coordinates)

    lonMin = lon
    latMin = lat

    lonMax = lonMin * 1.00001
    latMax = latMin * 1.0000001

    BBOX = f"{lonMin},{latMin},{lonMax},{latMax}"
    return BBOX


def get_lca(coordinates):
    BBOX = build_bbox(coordinates)

    url = f"""
    https://www.geoportal.de/openurl/https/geodienste.bfn.de/ogc/wms/schutzgebiet?SERVICE=WMS&\
    VERSION=1.3.0&\
    REQUEST=GetFeatureInfo&\
    FORMAT=image%2Fpng&\
    TRANSPARENT=true&\
    QUERY_LAYERS=Landschaftsschutzgebiete&\
    CACHEID=126191&\
    LAYERS=Landschaftsschutzgebiete&\
    SINGLETILE=false&\
    WIDTH=16&\
    HEIGHT=16&\
    INFO_FORMAT=text%2Fxml&\
    FEATURE_COUNT=1&\
    I=8&\
    J=8&\
    CRS=EPSG%3A25832&\
    STYLES=&\
    BBOX={BBOX}
    """

    url = url.replace(" ","")

    try:
        response = requests.get(url).text
        print(response)

        time.sleep(s)

        lca = False

        if "objectid" in response.lower():
            print(response)
            lca = True

    except Exception as e:
        lca = False

    return lca


def get_soilscore(coordinates):
    BBOX = build_bbox(coordinates)

    url = f"""https://www.geoportal.de/openurl/https/services.bgr.de/wms/boden/sqr1000/?SERVICE=WMS&\
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

    url = url.replace(" ","")

    headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "Host": "sg.geodatenzentrum.de",
    "Origin": "https://www.geoportal.de",
    "Referer": "https://www.geoportal.de/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
    "Sec-GPC": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.99 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers).text

        time.sleep(s)

        re_pv = re.compile(r'PixelValue="(.*?)"')
        pixel_value = re.findall(re_pv, response)

        soil_score = 0

        if pixel_value and len(pixel_value) != 0:
            if "nodata" not in str(pixel_value).lower():
                soil_score = float(pixel_value[0].replace(",","."))
            else:
                soil_score = 0

    except Exception as e:
        print(e)
        soil_score = False


    return soil_score



def get_topo(coordinates):
    BBOX = build_bbox(coordinates)

    url = f"""https://sg.geodatenzentrum.de/wms_dgm200__sess-0b9ad297-43d1-4a79-b751-677b69841816?SERVICE=WMS&\
    SERVICE=WMS&\
    VERSION=1.3.0&\
    REQUEST=GetFeatureInfo&\
    FORMAT=image%2Fpng&\
    TRANSPARENT=true&\
    QUERY_LAYERS=colormap&\
    CACHEID=2220043&\
    LAYERS=colormap&\
    SINGLETILE=false&\
    WIDTH=32&\
    HEIGHT=32&\
    INFO_FORMAT=text%2Fxml&\
    FEATURE_COUNT=1&\
    I=16&\
    J=16&\
    CRS=EPSG%3A25832&\
    STYLES=&\
    BBOX={BBOX}"""

    url = url.replace(" ","")


    headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "Host": "sg.geodatenzentrum.de",
    "Origin": "https://www.geoportal.de",
    "Referer": "https://www.geoportal.de/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
    "Sec-GPC": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.99 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers).text

        time.sleep(s)

        pattern = re.compile(r"<dgm200:hoehe>(.*?)</dgm200:hoehe>")

        topo = re.findall(pattern, response)
        print(topo)

        elevation = 0

        if topo and len(topo) != 0:
            if "nodata" not in str(topo).lower():
                elevation = float(topo[0].replace(",","."))
            else:
                elevation = 0

    except Exception as e:
        elevation = False

    return elevation
