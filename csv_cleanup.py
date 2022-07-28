import os, sys

regios = [
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
]


for regio in regios:

    file = f"/common/ecap/prospector_data/results/final/{regio}/csv/{regio}_final2.csv"

    cleaned = None

    with open(file, "r") as f:
        text = f.read()

        text = text.replace(",", ";").replace(".", ",")
        cleaned = text

    clean_file = (
        f"/common/ecap/prospector_data/results/final/{regio}/csv/{regio}_final.csv"
    )

    with open(clean_file, "w") as file:
        file.write(cleaned)
