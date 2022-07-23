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
            # "rheinland_pfalz",  # solar_done
            # "saarland",  # solar_done
            # "berlin",  # solar_done
            "bremen",  # solar_done
            # "hamburg",  # solar_done
            # "hessen",  # solar_done
            # "schleswig_holstein",  # solar_done
            # "brandenburg",  # solar_done
            # "bayern",  # todo
            # "nordrhein_westfalen",  # todo
            # "thueringen",  # todo
            # "niedersachsen",  # todo
            # "sachsen_anhalt",  # todo
            # "sachsen",  # todo
            # "baden_wuerttemberg",  # todo
            # "mecklenburg_vorpommern",  # todo
        ],
        stage="stage_6_5",
        finput=["input"],
    )

    print(timeit.default_timer() - t)
