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

use std::ops::Range;
use std::sync::Arc;

use arrow::array::BooleanBufferBuilder;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion_common::Result;
use geo_types::Rect;
use parking_lot::Mutex;
use sedona_common::ExecutionMode;
use sedona_expr::statistics::GeoStatistics;
use sedona_spatial_join::evaluated_batch::EvaluatedBatch;
use sedona_spatial_join::index::spatial_index::SpatialIndex;
use sedona_spatial_join::index::QueryResultMetrics;
use wkb::reader::Wkb;

/// GPU-accelerated [`SpatialIndex`] implementation (stub).
///
/// This is a placeholder for a future GPU-backed spatial index.
/// All methods currently return unimplemented errors.
#[derive(Debug)]
pub struct GpuSpatialIndex;

#[async_trait]
impl SpatialIndex for GpuSpatialIndex {
    fn schema(&self) -> SchemaRef {
        unimplemented!("GPU spatial index not yet implemented")
    }

    fn num_indexed_batches(&self) -> usize {
        unimplemented!("GPU spatial index not yet implemented")
    }

    fn get_indexed_batch(&self, _batch_idx: usize) -> &RecordBatch {
        unimplemented!("GPU spatial index not yet implemented")
    }

    fn query(
        &self,
        _probe_wkb: &Wkb,
        _probe_rect: &Rect<f32>,
        _distance: &Option<f64>,
        _build_batch_positions: &mut Vec<(i32, i32)>,
    ) -> Result<QueryResultMetrics> {
        unimplemented!("GPU spatial index not yet implemented")
    }

    fn query_knn(
        &self,
        _probe_wkb: &Wkb,
        _k: u32,
        _use_spheroid: bool,
        _include_tie_breakers: bool,
        _build_batch_positions: &mut Vec<(i32, i32)>,
        _distances: Option<&mut Vec<f64>>,
    ) -> Result<QueryResultMetrics> {
        unimplemented!("GPU spatial index not yet implemented")
    }

    async fn query_batch(
        &self,
        _evaluated_batch: &Arc<EvaluatedBatch>,
        _range: Range<usize>,
        _max_result_size: usize,
        _build_batch_positions: &mut Vec<(i32, i32)>,
        _probe_indices: &mut Vec<u32>,
    ) -> Result<(QueryResultMetrics, usize)> {
        unimplemented!("GPU spatial index not yet implemented")
    }

    fn need_more_probe_stats(&self) -> bool {
        false
    }

    fn merge_probe_stats(&self, _stats: GeoStatistics) {}

    fn visited_build_side(&self) -> Option<&Mutex<Vec<BooleanBufferBuilder>>> {
        None
    }

    fn report_probe_completed(&self) -> bool {
        true
    }

    fn get_refiner_mem_usage(&self) -> usize {
        0
    }

    fn get_actual_execution_mode(&self) -> ExecutionMode {
        ExecutionMode::PrepareNone
    }
}
