import sys
import os
import timeit

parent_dirs = ["validate", "filters", "util", "converter"]
for dir in parent_dirs:
    sys.path.append(f"prospector")


t = timeit.default_timer()
from prospector.prospector import Prospector

from prospector.display.display import Display

if __name__ == "__main__":
    print("\n")
    Display.start_screen()
    print("\n\n")

    proj_path = os.path.abspath(os.path.dirname(__file__))

    prospector = Prospector(proj_path, preload=False)
    prospector.staging(
        [
            # "saarland",
            "berlin",
            # "bremen",
            # "hamburg",
            # "hessen",
            # "rheinland_pfalz",
            "sachsen",
            # "schleswig_holstein",
            "brandenburg",
            # "bayern",
            # "nordrhein_westfalen",
            "thueringen",
            # "niedersachsen",
            "sachsen_anhalt",
            # "baden_wuerttemberg",
            "mecklenburg_vorpommern",
        ],
        stage="stage_7",
        finput=["input"],
    )

    print(timeit.default_timer() - t)
