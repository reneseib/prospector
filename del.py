import os
import sys


maindir = "/common/ecap/prospector_data/results/stages/6-added_slope"

for state in os.listdir(maindir):
    state_path = os.path.join(maindir, state, "gpkg")

    for file in os.listdir(state_path):
        file_path = os.path.join(state_path, file)
        os.remove(file_path)
