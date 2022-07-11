## GEOTREE

- Load file
- Iterate over file contents to fetch id & geometry
  - Load geometry from column "geomarr", which is always a list of lists:
    - Regular polygons have len(geomarr[0]) = 1, multipolygon have len(geomarr[0]) > 1
- Store data in typed dict
  {key=id : value:geometry as float64 array}

- **CENTROIDS already exist in the file.** We just need to add the bboxes for each geometry

## GENERAL

- File handling with geopandas
- Column and row handling with pandas
- Processing in Numba
