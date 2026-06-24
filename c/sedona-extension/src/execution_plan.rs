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

use std::{any::Any, sync::Arc};

use datafusion_common::{Result, Statistics};
use datafusion_execution::TaskContext;
use datafusion_physical_plan::{
    execution_plan::CardinalityEffect, metrics::MetricsSet, DisplayAs, DisplayFormatType,
    ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

#[derive(Debug)]
struct ImportedTableProviderExec {
    name: String,
    properties: Arc<PlanProperties>,
    children: Vec<Arc<dyn ExecutionPlan>>,
}

impl DisplayAs for ImportedTableProviderExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        todo!()
    }
}

impl ExecutionPlan for ImportedTableProviderExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        todo!()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        todo!()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        todo!()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        todo!()
    }

    fn supports_limit_pushdown(&self) -> bool {
        todo!()
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        todo!()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}
