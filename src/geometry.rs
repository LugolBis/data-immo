use serde_json::{Map, Value};

fn inside(point: &[f64; 2], split_value: f64, axis: &str, side: &str) -> bool {
    let value: f64;
    if axis == "x" {
        value = point[0];
    } else {
        value = point[1];
    }

    if ["left", "bottom"].contains(&side) {
        return value <= split_value;
    } else {
        return value >= split_value;
    }
}

fn compute_intersection(s: [f64; 2], p: [f64; 2], split_value: f64, axis: &str) -> [f64; 2] {
    if axis == "x" {
        let t = (split_value - s[0]) / (p[0] - s[0]);
        let y = s[1] + t * (p[1] - s[1]);
        [split_value, y]
    } else {
        let t = (split_value - s[1]) / (p[1] - s[1]);
        let x = s[0] + t * (p[0] - s[0]);
        [x, split_value]
    }
}

fn clip_polygon(
    polygon: &Vec<Vec<f64>>,
    split_value: f64,
    axis: &str,
    side: &str,
) -> Result<Vec<Vec<Vec<f64>>>, String> {
    let mut result: Vec<Vec<f64>> = Vec::new();
    let s = polygon.get(polygon.len() - 1).ok_or(
        format!(
            "Failed to get the last element of the polygon : polygon size : {}",
            polygon.len()
        )
    )?;

    let mut point_s: [f64; 2] = s[..]
        .try_into()
        .map_err(|_| format!("The following point hasn't the expected format : {:?}", s))?;

    for point in polygon {
        let point: [f64; 2] = point[..].try_into().map_err(|_| {
            format!(
                "The following point hasn't the expected format : {:?}",
                point
            )
        })?;

        if inside(&point, split_value, axis, side) {
            if !inside(&point_s, split_value, axis, side) {
                let intersection = compute_intersection(point_s, point, split_value, axis);
                result.push(intersection.to_vec());
            }
            result.push(point.to_vec());
        } else {
            if inside(&point_s, split_value, axis, side) {
                let intersection = compute_intersection(point_s, point, split_value, axis);
                result.push(intersection.to_vec());
            }
        }
        point_s = point;
    }

    // Close the Polygon if necessary
    if result[0] != result[result.len() - 1] {
        result.push(result[0].clone())
    }

    if result.len() >= 4 {
        let mut wraped_result = Vec::new();
        wraped_result.push(result);
        Ok(wraped_result)
    } else {
        Err("Inconsistant size : The Polygon need to has 4 points or more.".to_string())
    }
}

/// Safe way to unwrap the coordinates from the GeoJSON format.
pub fn unwrap_coordinates(value: &Vec<Value>) -> Vec<Vec<Vec<f64>>> {
    value
        .into_iter()
        .map(|value1| {
            if let Value::Array(vec1) = value1 {
                Ok(vec1
                    .into_iter()
                    .map(|value2| {
                        if let Value::Array(vec2) = value2 {
                            Ok(vec2
                                .into_iter()
                                .map(|value3| {
                                    if let Value::Number(nb) = value3 {
                                        nb.as_f64().ok_or(())
                                    } else {
                                        Err(())
                                    }
                                })
                                .flatten()
                                .collect::<Vec<f64>>())
                        } else {
                            Err(())
                        }
                    })
                    .flatten()
                    .collect::<Vec<Vec<f64>>>())
            } else {
                Err(())
            }
        })
        .flatten()
        .collect::<Vec<Vec<Vec<f64>>>>()
}

/// Return the **min** & **max** values for the X and Y axis.
fn min_max_coordinate(coordinates: &Vec<Vec<f64>>) -> Result<(f64, f64, f64, f64), String> {
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;

    if coordinates.len() < 1 {
        return Err("Inconsistant coordinates : Empty vector.".to_string());
    }

    for point in coordinates {
        let x = *point
            .get(0)
            .ok_or("Inconsistant coordinate : Empty point.")?;

        let y = *point
            .get(1)
            .ok_or("Inconsistant coordinate : The point is not 2D.")?;

        if min_x > x {
            min_x = x;
        } else if max_x < x {
            max_x = x
        }

        if min_y > y {
            min_y = y;
        } else if max_y < y {
            max_y = y
        }
    }

    Ok((min_x, max_x, min_y, max_y))
}

/// Split the geometry extracted from the GeoJSON object and split into two geometry.<br>
/// It's splitted with the **X** or **Y** axis depending on the largest.
pub fn split_geometry(geometry: &Value) -> Result<(Value, Value), String> {
    let geometry = geometry
        .as_object()
        .ok_or("Inconsistant Value in input. The input need to be a Value::Object().")?;

    if geometry.get("type").unwrap_or_default() != "Polygon" {
        return Err(
            "Inconsistant geometry type, this function only support Polygon type.".to_string(),
        );
    }

    let coordinates = geometry
        .get("coordinates")
        .ok_or("Inconsistant geometry : The key 'coordinates' doesn't exists.")?
        .as_array()
        .ok_or("Inconsistant geometry : The key 'coordinates' is not an Array<Value>.")?;
    let coordinates = unwrap_coordinates(coordinates);

    let exterior = coordinates
        .get(0)
        .ok_or("Inconsistant geometry : Empty coordinates.")?;

    let (min_x, max_x, min_y, max_y) = min_max_coordinate(exterior)?;
    let width = max_x - min_x;
    let height = max_y - min_y;

    let poly1: Vec<Vec<Vec<f64>>>;
    let poly2: Vec<Vec<Vec<f64>>>;
    if width > height {
        let split_value = (min_x + max_x) / 2f64;
        poly1 = clip_polygon(exterior, split_value, "x", "left")?;
        poly2 = clip_polygon(exterior, split_value, "x", "right")?;
    } else {
        let split_value = (min_y + max_y) / 2f64;
        poly1 = clip_polygon(exterior, split_value, "y", "bottom")?;
        poly2 = clip_polygon(exterior, split_value, "y", "top")?;
    }

    let mut geometry1 = Map::new();
    geometry1.insert("type".to_string(), "Polygon".into());
    geometry1.insert("coordinates".to_string(), poly1.into());

    let mut geometry2 = Map::new();
    geometry2.insert("type".to_string(), "Polygon".into());
    geometry2.insert("coordinates".to_string(), poly2.into());

    Ok((geometry1.into(), geometry2.into()))
}
