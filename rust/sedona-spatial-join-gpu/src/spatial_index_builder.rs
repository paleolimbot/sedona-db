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

use async_trait::async_trait;
use datafusion_common::Result;
use sedona_common::SpatialJoinOptions;
use sedona_expr::statistics::GeoStatistics;
use sedona_spatial_join::evaluated_batch::evaluated_batch_stream::SendableEvaluatedBatchStream;
use sedona_spatial_join::index::spatial_index::SpatialIndexRef;
use sedona_spatial_join::index::spatial_index_builder::SpatialIndexBuilder;
use sedona_spatial_join::spatial_predicate::SpatialPredicate;

/// GPU-accelerated [`SpatialIndexBuilder`] implementation (stub).
///
/// This is a placeholder for a future GPU-backed spatial index builder.
/// All methods currently return unimplemented errors.
#[derive(Debug)]
pub struct GpuSpatialIndexBuilder;

#[async_trait]
impl SpatialIndexBuilder for GpuSpatialIndexBuilder {
    fn estimate_extra_memory_usage(
        &self,
        _geo_stats: &GeoStatistics,
        _spatial_predicate: &SpatialPredicate,
        _options: &SpatialJoinOptions,
    ) -> usize {
        0
    }

    fn finish(self) -> Result<SpatialIndexRef> {
        unimplemented!("GPU spatial index builder not yet implemented")
    }

    async fn add_stream(
        &mut self,
        _stream: SendableEvaluatedBatchStream,
        _geo_statistics: GeoStatistics,
    ) -> Result<()> {
        unimplemented!("GPU spatial index builder not yet implemented")
    }
}
