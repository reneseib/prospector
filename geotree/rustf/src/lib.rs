use std::any::type_name;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple, PyFloat};
use pyo3::PyObject;
use pyo3::prelude::{pymodule, pyclass, PyResult, Python, PyModule};
use std::env;

fn type_of<T>(_: &T) {
    println!("{}", std::any::type_name::<T>())
}



#[pyfunction]
fn start(primary_data: &PyDict) -> () {
    let init_prime: Result<&PyDict, _> = primary_data.extract();
    let prime  = init_prime.unwrap();

    for x in 0 .. prime.keys().len() {
        let mut elem = prime.get_item(x).unwrap();
        let elem: Vec<Vec<f64>>  = elem.extract().unwrap();
        let sum: f64 = elem[0].iter().sum();
        println!("{:?}", sum);

        // for i in 0..elem.len() {
        //     let point: Result<&PyTuple, _> = Ok(elem.get_item(i).unwrap().downcast::<PyTuple>().);
        //     println!("{:?}\n", &point);

        // }
    }
}


#[pyfunction]
fn calculate_shortest_distance(current_polygon: Vec<Vec<f64>>, closest_pa_ids: Vec<f64>, secondary_data: &PyDict) -> f64 {
    let mut shortest_distances: Vec<f64> = Vec::new();

    let secondary_data = secondary_data.downcast::<PyDict>().unwrap();
    // let zero = secondary_data.get_item(0);
    // println!("{:?}", zero.unwrap().get_item(0).unwrap());

    for i in 0 .. current_polygon.len() {
        let mut p1: &Vec<f64> = &current_polygon[i];
        // println!("p1: {:?}\n", p1);

        for i in 0..closest_pa_ids.len() {
            let pa_id: f64 = closest_pa_ids[i];
            let pa_polygon_coords = secondary_data.get_item(pa_id).unwrap().downcast::<PyList>().unwrap();
            // let pa_polygon_coords = secondary_data.get_item(pa_id).unwrap();
            // println!("{:?}", pa_polygon_coords);

            let mut min_distance: f64 = 0.0;

            let is_in = pa_polygon_coords.contains(p1).unwrap();

            if is_in {
                min_distance = 0.0
            } else {
                let mut polygon_single_distances: Vec<f64> = vec![];


                for x in 0 .. pa_polygon_coords.len() {
                    let p1: [f64; 2] = [p1[0], p1[1]];
                    let p2 = &pa_polygon_coords[x];
                    let p2: [f64; 2] = p2.extract().unwrap();

                    // println!("p2: {:?}\n", p2);

                    let rp1: [f64; 2] = [p1[0], p1[1]];
                    let rp2: [f64; 2] = [p2[0], p2[1]];

                    let d: f64 = rs_get_distance(p1, p2);
                    polygon_single_distances.push(d);
                    min_distance = polygon_single_distances.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                    // println!("{}\n", min_distance);

                }
            }

            shortest_distances.push(min_distance);
        }

    }
    // println!("\n-----------\n");
    let last_min_distance = shortest_distances.iter().fold(f64::INFINITY, |a, &b| a.min(b));
    // println!("last min distance: {}", last_min_distance);
    return last_min_distance
}


#[pyfunction]
fn rs_get_distance(point1: [f64; 2], point2: [f64; 2]) -> f64 {
    let p1: [f64;  2] = point1;
    let p2: [f64;  2] = point2;
    let [x1,y1] = p1;
    let [x2,y2] = p2;

    let a = (x1 - x2).abs();
    let b = (y1 - y2).abs();
    let c = (a.powf(2.) + b.powf(2.)).sqrt();
    return c;
}


/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "rustf")]
fn rustf(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rs_get_distance, m)?)?;
    m.add_function(wrap_pyfunction!(calculate_shortest_distance, m)?)?;
    m.add_function(wrap_pyfunction!(start, m)?)?;
    Ok(())
}
