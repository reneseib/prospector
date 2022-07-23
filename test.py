import os, sys
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

csv_file = "/common/ecap/prospector_data/src_data/power/csv/substations-germany.csv"

gpkg_file = "/common/ecap/prospector_data/src_data/power/gpkg/substations-germany.gpkg"

df = pd.read_csv(csv_file, header=0, sep=";")


def to_tuple(str):
    l = str.split(", ")
    l = [float(x) for x in l]
    t = tuple([l[1], l[0]])
    return t


df["geometry"] = df["Koordinaten"].apply(lambda x: Point(to_tuple(x)))

for i in range(len(df)):
    row = df.loc[i]
    if row["380 kV"] == "x":
        df.at[i, "380 kV"] = True
    else:
        df.at[i, "380 kV"] = False
    if row["220 kV"] == "x":
        df.at[i, "220 kV"] = True
    else:
        df.at[i, "220 kV"] = False

gdf = gpd.GeoDataFrame(df, geometry="geometry")
gdf = gdf.set_crs(4326).to_crs(25832)

print(gdf)
gdf.to_file(gpkg_file, driver="GPKG")

#
# regios = [
#     "baden_wuerttemberg",
#     "bayern",
#     "brandenburg",
#     "berlin",
#     "bremen",
#     "hamburg",
#     "hessen",
#     "bayern",
#     "niedersachsen",
#     "nordrhein_westfalen",
#     "rheinland_pfalz",
#     "saarland",
#     "sachsen_anhalt",
#     "sachsen",
#     "schleswig_holstein",
#     "thueringen",
# ]
#
# if not os.path.isdir(base_dir):
#     os.mkdir(base_dir)
#
# for regio in regios:
#     regio_dir = os.path.join(base_dir, regio)
#     if not os.path.isdir(regio_dir):
#         os.mkdir(regio_dir)
