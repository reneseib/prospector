import sys
import os

parent_dirs = ["validate", "filters", "util", "converter"]
for dir in parent_dirs:
    sys.path.append(f"prospector")

from prospector.prospector import Prospector

from prospector.display.display import Display

if __name__ == "__main__":
    print("\n")
    Display.start_screen()
    print("\n\n")

    proj_path = os.path.abspath(os.path.dirname(__file__))

    prospector = Prospector(proj_path, preload=False)
    prospector.staging(["brandenburg"], stage="stage_6", finput=["input"])
