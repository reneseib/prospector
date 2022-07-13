use polars::prelude::*;
use polars::df;
use arrow2::io::ipc::{{read::{FileReader, read_file_metadata}}, {write::{FileWriter, WriteOptions}}};


fn bla() -> Result<DataFrame> {
    let reader: FileReader = FileReader::new();
    let res = reader("/common/ecap/prospector_data/results/stages/3-filtered_by_intersection_protected_area/saarland/gpkg/saarland-3-filtered_by_intersection_protected_area.feather".into())
        .finish()
        // .filter(col("bar").gt(lit(100)))
        // .groupby(vec![col("ham")])
        // .agg(vec![col("spam").sum(), col("ham").sort(false).first()])
        .collect()
    return res;
}
