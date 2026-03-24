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

use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use sedona_query_planner::spatial_join_factory::{PlanSpatialJoinArgs, SpatialJoinFactory};

/// GPU-accelerated [`SpatialJoinFactory`] implementation (stub).
///
/// This factory is a placeholder for a future GPU-backed spatial join.
/// It currently returns `Ok(None)` for all inputs, allowing the planner
/// to fall through to the next registered factory.
#[derive(Debug)]
pub struct GpuSpatialJoinFactory;

impl GpuSpatialJoinFactory {
    /// Create a new GPU join factory.
    pub fn new() -> Self {
        Self
    }
}

impl Default for GpuSpatialJoinFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl SpatialJoinFactory for GpuSpatialJoinFactory {
    fn plan_spatial_join(
        &self,
        _args: &PlanSpatialJoinArgs<'_>,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // TODO: implement GPU-accelerated spatial join
        Ok(None)
    }
}
