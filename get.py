import shutil
import os
import sys


dir = "/common/ecap/prospector_data/results/stages/3-filtered_by_intersection_protected_area"

for regio in os.listdir(dir):
    regio_path = os.path.join(dir, regio, "gpkg")

    for file in os.listdir(regio_path):
        if file.endswith(".gpkg"):
            src = os.path.join(regio_path, file)
            dst = src.replace(
                "3-filtered_by_intersection_protected_area",
                "4-added_nearest_protected_area",
            )
            # print(dst)
            shutil.copyfile(src, dst)
