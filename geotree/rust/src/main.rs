use polars::prelude::*;

fn main() -> Result<DataFrame> {
    LazyCsvReader::new(("/common/ecap/prospector_data/results/stages/3-filtered_by_intersection_protected_area/{}/gpkg/{}-3-filtered_by_intersection_protected_area.feather","saarland","saarland").into())
        .finish()
        // .filter(col("bar").gt(lit(100)))
        // .groupby(vec![col("ham")])
        // .agg(vec![col("spam").sum(), col("ham").sort(false).first()])
        .collect()
}

