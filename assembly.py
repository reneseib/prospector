import os
import sys
import geopandas

from pconfig import config

src_dir = "/common/ecap/prospector_data/results/stages"
target_dir = "/common/ecap/prospector_data/results/final"

stages = list(config["directories"]["prospector_data"]["results"]["stages"].keys())

regios = list(config["area"]["subregions"])

c = 0

for stage in stages:
    stage_dir = os.path.join(src_dir, stage)

    if os.path.exists(stage_dir):
        for regio in regios:
            regio_dir = os.path.join(stage_dir, regio)

            if os.path.exists(regio_dir):
                gpkg_regio_dir = os.path.join(regio_dir, "gpkg")

                if os.path.exists(gpkg_regio_dir):
                    print(gpkg_regio_dir)
                    c += 1

print(c)
