import os, sys
import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon, mapping
import numpy as np


arr = np.array(
    [
        np.array([1.0, 20.0]).astype(np.float64),
        np.array([10.0, 2.0]).astype(np.float64),
        np.array([5.0, 25.0]).astype(np.float64),
    ]
).reshape(3, 2)
print(arr)

x = [x[0] for x in arr]
minx = min(x)
maxx = max(x)
y = [x[1] for x in arr]
miny = min(y)
maxy = max(y)

print(minx, maxx, miny, maxy)


# file = "/common/ecap/prospector_data/results/stages/5-added_nearest_substation/bremen/gpkg/bremen-5-added_nearest_substation.gpkg"
#
# data = gpd.read_file(file)
#
# gdf = gpd.GeoDataFrame(data)
#
#
# for i in range(len(gdf)):
#     row = gdf.iloc[i]
#     geometry = row["geometry"]
#
#     if type(geometry) == Polygon:
#         coords = list(geometry.exterior.coords)
#         coords = np.array([np.array(x).astype(np.float64) for x in coords]).astype(
#             np.float64
#         )
# min = np.amin(coords, axis=0)
# max = np.amax(coords, axis=0)
#
#         print(min, max)
