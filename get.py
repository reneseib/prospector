import numpy as np
import polars as pl

bla = pl.read_csv(
    "/Users/shellsquid/dev/green/geodata/FINAL/2-after_adding_centroid/csv/bayern.csv",
    delimiter=";",
    decimal=",",
)

print(len(bla))
# bla["area_ha"] = bla["area_ha"].apply(lambda x: float(x.replace(",", ".")))
#
# bla = bla.filter(pl.col("area_ha") > 25)
#
# np.random.
#
# bla = bla["area_ha"].apply(
#     lambda x: np.sqrt(
#         x * (np.array([0.28734782487, 2.3478874, 813.723476877659]) ** 1.1)
#     )
# )
# print(bla)


#
#
# for file in os.listdir(base_dir):
#     if file.endswith(".gpkg"):
#
#         file_path = os.path.join(base_dir, file)
#
#         target_fname = file.replace(".gpkg", ".feather")
#         out_path = os.path.join(base_dir, target_fname)
#         if not os.path.exists(out_path):
#             data = gpd.read_file(file_path)
#             data["geomarr"] = data["geometry"].apply(lambda x: convert(x))
#             data.to_feather(out_path)
#             print(f">> {target_fname} created")
# print("")
