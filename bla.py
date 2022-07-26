# import os
#
# from pconfig import config
#
# dir = "/common/ecap/prospector_data/results/final"
#
# regios = list(config["area"]["subregions"])
#
# for regio in regios:
#     regio_dir = os.path.join(dir, regio)
#
#     if not os.path.exists(regio_dir):
#         os.mkdir(regio_dir)
#
#     gpkg_dir = os.path.join(regio_dir, "gpkg")
#     if not os.path.exists(gpkg_dir):
#         os.mkdir(gpkg_dir)
