// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
use std::sync::Arc;

use arrow_array::builder::Float64Builder;
use arrow_schema::DataType;
use datafusion_common::error::Result;
use datafusion_expr::ColumnarValue;
use geo::HausdorffDistance;
use geo_types::Geometry;
use sedona_expr::{
    item_crs::ItemCrsKernel,
    scalar_udf::{ScalarKernelRef, SedonaScalarKernel},
};
use sedona_geo_generic_alg::HasDimensions;
use sedona_schema::{datatypes::SedonaType, matchers::ArgMatcher};

use crate::to_geo::GeoTypesExecutor;

/// ST_HausdorffDistanceVertices(geometry, geometry) implementation using geo crate
///
/// This function computes the discrete Hausdorff distance between two geometries,
/// considering only the vertices of the geometries rather than the continuous
/// Hausdorff distance that considers all points along edges.
pub fn st_hausdorff_distance_vertices_impl() -> Vec<ScalarKernelRef> {
    ItemCrsKernel::wrap_impl(STHausdorffDistanceVertices {})
}

#[derive(Debug)]
struct STHausdorffDistanceVertices {}

impl SedonaScalarKernel for STHausdorffDistanceVertices {
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>> {
        let matcher = ArgMatcher::new(
            vec![ArgMatcher::is_geometry(), ArgMatcher::is_geometry()],
            SedonaType::Arrow(DataType::Float64),
        );

        matcher.match_args(args)
    }

    fn invoke_batch(
        &self,
        arg_types: &[SedonaType],
        args: &[ColumnarValue],
    ) -> Result<ColumnarValue> {
        let executor = GeoTypesExecutor::new(arg_types, args);
        let mut builder = Float64Builder::with_capacity(executor.num_iterations());
        executor.execute_wkb_wkb_void(|lhs, rhs| {
            match (lhs, rhs) {
                (Some(lhs), Some(rhs)) => {
                    if let Some(distance) = invoke_scalar(lhs, rhs) {
                        builder.append_value(distance);
                    } else {
                        builder.append_null();
                    }
                }
                _ => builder.append_null(),
            }

            Ok(())
        })?;

        executor.finish(Arc::new(builder.finish()))
    }
}

fn invoke_scalar(lhs: &Geometry, rhs: &Geometry) -> Option<f64> {
    // Return NULL for empty geometries (PostGIS compatibility)
    if lhs.is_empty() || rhs.is_empty() {
        return None;
    }

    // Compute discrete Hausdorff distance using the geo crate
    Some(lhs.hausdorff_distance(rhs))
}

#[cfg(test)]
mod tests {
    use datafusion_common::ScalarValue;
    use rstest::rstest;
    use sedona_expr::scalar_udf::SedonaScalarUDF;
    use sedona_schema::datatypes::{WKB_GEOMETRY, WKB_GEOMETRY_ITEM_CRS, WKB_VIEW_GEOMETRY};
    use sedona_testing::testers::ScalarUdfTester;

    use super::*;

    #[rstest]
    fn udf(#[values(WKB_GEOMETRY, WKB_VIEW_GEOMETRY)] sedona_type: SedonaType) {
        let udf = SedonaScalarUDF::from_impl(
            "st_hausdorffdistancevertices",
            st_hausdorff_distance_vertices_impl(),
        );
        let tester =
            ScalarUdfTester::new(udf.into(), vec![sedona_type.clone(), sedona_type.clone()]);
        tester.assert_return_type(DataType::Float64);

        // Point to point - Hausdorff distance equals Euclidean distance
        let result = tester
            .invoke_scalar_scalar("POINT (0 0)", "POINT (3 4)")
            .unwrap();
        tester.assert_scalar_result_equals(result, 5.0);

        // NULL handling
        let result = tester
            .invoke_scalar_scalar(ScalarValue::Null, ScalarValue::Null)
            .unwrap();
        assert!(result.is_null());

        let result = tester
            .invoke_scalar_scalar("POINT (0 0)", ScalarValue::Null)
            .unwrap();
        assert!(result.is_null());

        let result = tester
            .invoke_scalar_scalar(ScalarValue::Null, "POINT (0 0)")
            .unwrap();
        assert!(result.is_null());

        // EMPTY geometries return NULL (PostGIS compatibility)
        // Note: Using LINESTRING EMPTY since POINT EMPTY cannot be represented by geo_types
        let result = tester
            .invoke_scalar_scalar("LINESTRING EMPTY", "POINT (0 0)")
            .unwrap();
        assert!(result.is_null());

        let result = tester
            .invoke_scalar_scalar("LINESTRING EMPTY", "LINESTRING EMPTY")
            .unwrap();
        assert!(result.is_null());

        // LineString to LineString
        // Discrete Hausdorff distance considers only vertices:
        // First line: (0,0), (2,0)
        // Second line: (0,1), (1,1), (2,1)
        // Point (1,1) is sqrt(2) away from nearest vertex on first line
        let result = tester
            .invoke_scalar_scalar("LINESTRING (0 0, 2 0)", "LINESTRING (0 1, 1 1, 2 1)")
            .unwrap();
        if let ScalarValue::Float64(Some(distance)) = result {
            assert!((distance - std::f64::consts::SQRT_2).abs() < 1e-10);
        } else {
            panic!("Expected Float64 result");
        }

        // Polygon to Polygon
        let result = tester
            .invoke_scalar_scalar(
                "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",
                "POLYGON ((2 0, 3 0, 3 1, 2 1, 2 0))",
            )
            .unwrap();
        tester.assert_scalar_result_equals(result, 2.0);
    }

    #[rstest]
    fn udf_item_crs(
        #[values(WKB_GEOMETRY_ITEM_CRS.clone())] left_sedona_type: SedonaType,
        #[values(WKB_GEOMETRY_ITEM_CRS.clone())] right_sedona_type: SedonaType,
    ) {
        let udf = SedonaScalarUDF::from_impl(
            "st_hausdorffdistancevertices",
            st_hausdorff_distance_vertices_impl(),
        );
        let tester = ScalarUdfTester::new(
            udf.into(),
            vec![left_sedona_type.clone(), right_sedona_type.clone()],
        );

        assert_eq!(
            tester.return_type().unwrap(),
            SedonaType::Arrow(DataType::Float64)
        );

        // Point to point - Hausdorff distance equals Euclidean distance (3-4-5 triangle)
        let result = tester
            .invoke_scalar_scalar("POINT (0 0)", "POINT (3 4)")
            .unwrap();
        if let ScalarValue::Float64(Some(distance)) = result {
            assert!((distance - 5.0).abs() < 1e-10);
        } else {
            panic!("Expected Float64 result");
        }
    }
}
