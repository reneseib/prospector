use pyo3::prelude::*;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn rs_get_distance(point1: [f64; 2], point2: [f64; 2]) -> () {
    let p1: [f64;  2] = point1;
    let p2: [f64;  2] = point2;
    let [x1,y1] = p1;
    let [x2,y2] = p2;

    let a = (x1 - x2).abs();
    let b = (y1 - y2).abs();
    let c = (a.powf(2.) + b.powf(2.)).sqrt();
    println!("{}", c);
}

/// A Python module implemented in Rust.
#[pymodule]
fn rsgeotree(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rs_get_distance, m)?)?;
    Ok(())
}
