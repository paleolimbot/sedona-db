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
use sedona_query_planner::spatial_predicate::SpatialPredicate;
use sedona_spatial_join::factory::{is_spatial_predicate_supported, repartition_probe_side, should_swap_join_order};
use crate::exec::SpatialJoinExec;

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
        args: &PlanSpatialJoinArgs<'_>,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if !is_spatial_predicate_supported(
            args.spatial_predicate,
            &args.physical_left.schema(),
            &args.physical_right.schema(),
        )? {
            return Ok(None);
        }

        let should_swap = !matches!(
            args.spatial_predicate,
            SpatialPredicate::KNearestNeighbors(_)
        ) && args.join_type.supports_swap()
            && should_swap_join_order(args.physical_left.as_ref(), args.physical_right.as_ref())?;

        // Repartition the probe side when enabled. This breaks spatial locality in sorted/skewed
        // datasets, leading to more balanced workloads during out-of-core spatial join.
        // We determine which pre-swap input will be the probe AFTER any potential swap, and
        // repartition it here. swap_inputs() will then carry the RepartitionExec to the correct
        // child position.
        let (physical_left, physical_right) = if args.join_options.repartition_probe_side {
            repartition_probe_side(
                args.physical_left.clone(),
                args.physical_right.clone(),
                args.spatial_predicate,
                should_swap,
            )?
        } else {
            (args.physical_left.clone(), args.physical_right.clone())
        };

        let exec = SpatialJoinExec::try_new(
            physical_left,
            physical_right,
            args.spatial_predicate.clone(),
            args.remainder.cloned(),
            args.join_type,
            None,
            args.join_options,
        )?;

        if should_swap {
            exec.swap_inputs().map(Some)
        } else {
            Ok(Some(Arc::new(exec) as Arc<dyn ExecutionPlan>))
        }
    }
}
