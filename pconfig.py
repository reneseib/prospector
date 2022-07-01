config = {
    "init": {
        "prospector_package_path": [
            "/common/ecap/prospector",
            "/Users/shellsquid/dev/osm/prospector/",
        ],
        "proj_path": [
            "/common/ecap",
            "/Users/shellsquid/dev/osm",
        ],
    },
    "epsg": {
        "baden_wuerttemberg": 25832,
        "bayern": 25832,
        "brandenburg": 25833,
        "berlin": 25833,
        "bremen": 25832,
        "hamburg": 25832,
        "hessen": 25832,
        "mecklenburg_vorpommern": 25832,
        "niedersachsen": 25832,
        "nordrhein_westfalen": 25832,
        "rheinland_pfalz": 25832,
        "saarland": 25832,
        "sachsen_anhalt": 25833,
        "sachsen": 25833,
        "schleswig_holstein": 25832,
        "thueringen": 25833,
    },
    "area": {
        "continent": "Europe",
        "country": "Germany",
        "subregions": [
            "baden_wuerttemberg",
            "bayern",
            "brandenburg",
            "berlin",
            "bremen",
            "hamburg",
            "hessen",
            "mecklenburg_vorpommern",
            "niedersachsen",
            "nordrhein_westfalen",
            "rheinland_pfalz",
            "saarland",
            "sachsen_anhalt",
            "sachsen",
            "schleswig_holstein",
            "thueringen",
        ],
        "crs": 4326,
    },
    "prot_areas": [
        "lsg_gesamt_de",
        "fauna_flora_habitat_gesamt_de",
        "nsg_gesamt_de",
        "vogelschutz_gesamt_de",
        "naturparks_gesamt_de",
        "biosphaeren_gesamt_de",
        "nationalparks_gesamt_de",
        "naturmonumente_gesamt_de",
    ],
    "directories": {
        "prospector_data": {
            "src_data": {
                "geo_data": {
                    "baden_wuerttemberg": ("csv", "gpkg", "osm"),
                    "bayern": ("csv", "gpkg", "osm"),
                    "brandenburg": ("csv", "gpkg", "osm"),
                    "berlin": ("csv", "gpkg", "osm"),
                    "bremen": ("csv", "gpkg", "osm"),
                    "hamburg": ("csv", "gpkg", "osm"),
                    "hessen": ("csv", "gpkg", "osm"),
                    "mecklenburg_vorpommern": ("csv", "gpkg", "osm"),
                    "niedersachsen": ("csv", "gpkg", "osm"),
                    "nordrhein_westfalen": ("csv", "gpkg", "osm"),
                    "rheinland_pfalz": ("csv", "gpkg", "osm"),
                    "saarland": ("csv", "gpkg", "osm"),
                    "sachsen_anhalt": ("csv", "gpkg", "osm"),
                    "sachsen": ("csv", "gpkg", "osm"),
                    "schleswig_holstein": ("csv", "gpkg", "osm"),
                    "thueringen": ("csv", "gpkg", "osm"),
                },
                "protected_areas": ("csv", "gpkg"),
                "power": ("csv", "gpkg"),
            },
            "results": {
                "final": ("csv", "gpkg"),
                "stages": {
                    "0-filtered_by_landuse": {
                        "baden_wuerttemberg": ("csv", "gpkg"),
                        "bayern": ("csv", "gpkg"),
                        "brandenburg": ("csv", "gpkg"),
                        "berlin": ("csv", "gpkg"),
                        "bremen": ("csv", "gpkg"),
                        "hamburg": ("csv", "gpkg"),
                        "hessen": ("csv", "gpkg"),
                        "mecklenburg_vorpommern": ("csv", "gpkg"),
                        "niedersachsen": ("csv", "gpkg"),
                        "nordrhein_westfalen": ("csv", "gpkg"),
                        "rheinland_pfalz": ("csv", "gpkg"),
                        "saarland": ("csv", "gpkg"),
                        "sachsen_anhalt": ("csv", "gpkg"),
                        "sachsen": ("csv", "gpkg"),
                        "schleswig_holstein": ("csv", "gpkg"),
                        "thueringen": ("csv", "gpkg"),
                    },
                    "1-filtered_by_size": {
                        "baden_wuerttemberg": ("csv", "gpkg"),
                        "bayern": ("csv", "gpkg"),
                        "brandenburg": ("csv", "gpkg"),
                        "berlin": ("csv", "gpkg"),
                        "bremen": ("csv", "gpkg"),
                        "hamburg": ("csv", "gpkg"),
                        "hessen": ("csv", "gpkg"),
                        "mecklenburg_vorpommern": ("csv", "gpkg"),
                        "niedersachsen": ("csv", "gpkg"),
                        "nordrhein_westfalen": ("csv", "gpkg"),
                        "rheinland_pfalz": ("csv", "gpkg"),
                        "saarland": ("csv", "gpkg"),
                        "sachsen_anhalt": ("csv", "gpkg"),
                        "sachsen": ("csv", "gpkg"),
                        "schleswig_holstein": ("csv", "gpkg"),
                        "thueringen": ("csv", "gpkg"),
                    },
                    "2-added_centroids": {
                        "baden_wuerttemberg": ("csv", "gpkg"),
                        "bayern": ("csv", "gpkg"),
                        "brandenburg": ("csv", "gpkg"),
                        "berlin": ("csv", "gpkg"),
                        "bremen": ("csv", "gpkg"),
                        "hamburg": ("csv", "gpkg"),
                        "hessen": ("csv", "gpkg"),
                        "mecklenburg_vorpommern": ("csv", "gpkg"),
                        "niedersachsen": ("csv", "gpkg"),
                        "nordrhein_westfalen": ("csv", "gpkg"),
                        "rheinland_pfalz": ("csv", "gpkg"),
                        "saarland": ("csv", "gpkg"),
                        "sachsen_anhalt": ("csv", "gpkg"),
                        "sachsen": ("csv", "gpkg"),
                        "schleswig_holstein": ("csv", "gpkg"),
                        "thueringen": ("csv", "gpkg"),
                    },
                    "3-filtered_by_intersection_protected_area": {
                        "baden_wuerttemberg": ("csv", "gpkg"),
                        "bayern": ("csv", "gpkg"),
                        "brandenburg": ("csv", "gpkg"),
                        "berlin": ("csv", "gpkg"),
                        "bremen": ("csv", "gpkg"),
                        "hamburg": ("csv", "gpkg"),
                        "hessen": ("csv", "gpkg"),
                        "mecklenburg_vorpommern": ("csv", "gpkg"),
                        "niedersachsen": ("csv", "gpkg"),
                        "nordrhein_westfalen": ("csv", "gpkg"),
                        "rheinland_pfalz": ("csv", "gpkg"),
                        "saarland": ("csv", "gpkg"),
                        "sachsen_anhalt": ("csv", "gpkg"),
                        "sachsen": ("csv", "gpkg"),
                        "schleswig_holstein": ("csv", "gpkg"),
                        "thueringen": ("csv", "gpkg"),
                    },
                    "4-added_nearest_protected_area": {
                        "baden_wuerttemberg": ("csv", "gpkg"),
                        "bayern": ("csv", "gpkg"),
                        "brandenburg": ("csv", "gpkg"),
                        "berlin": ("csv", "gpkg"),
                        "bremen": ("csv", "gpkg"),
                        "hamburg": ("csv", "gpkg"),
                        "hessen": ("csv", "gpkg"),
                        "mecklenburg_vorpommern": ("csv", "gpkg"),
                        "niedersachsen": ("csv", "gpkg"),
                        "nordrhein_westfalen": ("csv", "gpkg"),
                        "rheinland_pfalz": ("csv", "gpkg"),
                        "saarland": ("csv", "gpkg"),
                        "sachsen_anhalt": ("csv", "gpkg"),
                        "sachsen": ("csv", "gpkg"),
                        "schleswig_holstein": ("csv", "gpkg"),
                        "thueringen": ("csv", "gpkg"),
                    },
                    "5-added_nearest_substation": {
                        "baden_wuerttemberg": ("csv", "gpkg"),
                        "bayern": ("csv", "gpkg"),
                        "brandenburg": ("csv", "gpkg"),
                        "berlin": ("csv", "gpkg"),
                        "bremen": ("csv", "gpkg"),
                        "hamburg": ("csv", "gpkg"),
                        "hessen": ("csv", "gpkg"),
                        "mecklenburg_vorpommern": ("csv", "gpkg"),
                        "niedersachsen": ("csv", "gpkg"),
                        "nordrhein_westfalen": ("csv", "gpkg"),
                        "rheinland_pfalz": ("csv", "gpkg"),
                        "saarland": ("csv", "gpkg"),
                        "sachsen_anhalt": ("csv", "gpkg"),
                        "sachsen": ("csv", "gpkg"),
                        "schleswig_holstein": ("csv", "gpkg"),
                        "thueringen": ("csv", "gpkg"),
                    },
                    "6-added_slope": {
                        "baden_wuerttemberg": ("csv", "gpkg"),
                        "bayern": ("csv", "gpkg"),
                        "brandenburg": ("csv", "gpkg"),
                        "berlin": ("csv", "gpkg"),
                        "bremen": ("csv", "gpkg"),
                        "hamburg": ("csv", "gpkg"),
                        "hessen": ("csv", "gpkg"),
                        "mecklenburg_vorpommern": ("csv", "gpkg"),
                        "niedersachsen": ("csv", "gpkg"),
                        "nordrhein_westfalen": ("csv", "gpkg"),
                        "rheinland_pfalz": ("csv", "gpkg"),
                        "saarland": ("csv", "gpkg"),
                        "sachsen_anhalt": ("csv", "gpkg"),
                        "sachsen": ("csv", "gpkg"),
                        "schleswig_holstein": ("csv", "gpkg"),
                        "thueringen": ("csv", "gpkg"),
                    },
                    "7-added_nearest_agrargen": {
                        "baden_wuerttemberg": ("csv", "gpkg"),
                        "bayern": ("csv", "gpkg"),
                        "brandenburg": ("csv", "gpkg"),
                        "berlin": ("csv", "gpkg"),
                        "bremen": ("csv", "gpkg"),
                        "hamburg": ("csv", "gpkg"),
                        "hessen": ("csv", "gpkg"),
                        "mecklenburg_vorpommern": ("csv", "gpkg"),
                        "niedersachsen": ("csv", "gpkg"),
                        "nordrhein_westfalen": ("csv", "gpkg"),
                        "rheinland_pfalz": ("csv", "gpkg"),
                        "saarland": ("csv", "gpkg"),
                        "sachsen_anhalt": ("csv", "gpkg"),
                        "sachsen": ("csv", "gpkg"),
                        "schleswig_holstein": ("csv", "gpkg"),
                        "thueringen": ("csv", "gpkg"),
                    },
                    "8-added_solar_data": {
                        "baden_wuerttemberg": ("csv", "gpkg"),
                        "bayern": ("csv", "gpkg"),
                        "brandenburg": ("csv", "gpkg"),
                        "berlin": ("csv", "gpkg"),
                        "bremen": ("csv", "gpkg"),
                        "hamburg": ("csv", "gpkg"),
                        "hessen": ("csv", "gpkg"),
                        "mecklenburg_vorpommern": ("csv", "gpkg"),
                        "niedersachsen": ("csv", "gpkg"),
                        "nordrhein_westfalen": ("csv", "gpkg"),
                        "rheinland_pfalz": ("csv", "gpkg"),
                        "saarland": ("csv", "gpkg"),
                        "sachsen_anhalt": ("csv", "gpkg"),
                        "sachsen": ("csv", "gpkg"),
                        "schleswig_holstein": ("csv", "gpkg"),
                        "thueringen": ("csv", "gpkg"),
                    },
                },
            },
        }
    },
}
