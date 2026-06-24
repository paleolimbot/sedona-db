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

use std::{
    any::Any,
    ffi::{c_int, c_void, CStr},
    fmt::Debug,
    ptr::null_mut,
    sync::Arc,
};

use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_schema::{ffi::FFI_ArrowSchema, Schema, SchemaRef};
use datafusion_common::{exec_err, Result, Statistics};
use datafusion_execution::TaskContext;
use datafusion_physical_plan::{
    execution_plan::CardinalityEffect, metrics::MetricsSet, DisplayAs, DisplayFormatType,
    ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream,
};
use sedona_common::{sedona_internal_datafusion_err, sedona_internal_err};
use serde::{Deserialize, Serialize};

use crate::extension::{SedonaCError, SedonaCExecutionPlan, SedonaCExecutionPlanArgs};
use crate::utils::{
    ffi_stream_to_sendable, get_plan_property, get_plan_string_property, StreamingRecordBatchReader,
    ERRNO_OK,
};

/// Arguments for executing a partition of an execution plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteArgs {
    pub partition: usize,
}

/// Properties of an execution plan serialized across FFI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanPropertiesArgs {
    pub num_partitions: usize,
    pub supports_limit_pushdown: bool,
}

impl PlanPropertiesArgs {
    /// Extract plan properties from a SedonaCExecutionPlan via FFI.
    pub fn from_ffi_plan(plan: &SedonaCExecutionPlan) -> Result<Self> {
        get_plan_property::<Self, ()>(plan, "plan_properties", None)
    }

    /// Convert to DataFusion PlanProperties.
    pub fn into_plan_properties(self, schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            datafusion_physical_expr::EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(self.num_partitions),
            datafusion_physical_plan::execution_plan::EmissionType::Incremental,
            datafusion_physical_plan::execution_plan::Boundedness::Bounded,
        )
    }
}

/// Helper wrapper to format an ExecutionPlan with a specific DisplayFormatType.
struct DisplayAsWrapper<'a>(&'a Arc<dyn ExecutionPlan>, DisplayFormatType);

impl std::fmt::Display for DisplayAsWrapper<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt_as(self.1, f)
    }
}

/// Wrapper around an [ExecutionPlan] that can be exported across FFI.
pub struct ExportedExecutionPlan {
    plan: Arc<dyn ExecutionPlan>,
    task_context: Arc<TaskContext>,
    runtime: tokio::runtime::Handle,
}

impl Debug for ExportedExecutionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExportedExecutionPlan")
            .field("plan", &self.plan)
            .finish()
    }
}

impl ExportedExecutionPlan {
    /// Create a new ExportedExecutionPlan from an ExecutionPlan.
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        task_context: Arc<TaskContext>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            plan,
            task_context,
            runtime,
        }
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn get_property(&self, property: &str) -> Result<Vec<u8>> {
        match property {
            "plan_properties" => {
                let props = PlanPropertiesArgs {
                    num_partitions: self
                        .plan
                        .properties()
                        .output_partitioning()
                        .partition_count(),
                    supports_limit_pushdown: self.plan.supports_limit_pushdown(),
                };
                serde_json::to_vec(&props).map_err(|e| {
                    sedona_internal_datafusion_err!("Failed to serialize plan properties: {}", e)
                })
            }
            "debug_string" => Ok(format!("{:?}", self.plan).into_bytes()),
            "display_default" => {
                use std::fmt::Write;
                let mut s = String::new();
                let _ = write!(s, "{}", DisplayAsWrapper(&self.plan, DisplayFormatType::Default));
                Ok(s.into_bytes())
            }
            "display_verbose" => {
                use std::fmt::Write;
                let mut s = String::new();
                let _ = write!(s, "{}", DisplayAsWrapper(&self.plan, DisplayFormatType::Verbose));
                Ok(s.into_bytes())
            }
            "display_tree_render" => {
                use std::fmt::Write;
                let mut s = String::new();
                let _ = write!(s, "{}", DisplayAsWrapper(&self.plan, DisplayFormatType::TreeRender));
                Ok(s.into_bytes())
            }
            "name" => Ok(self.plan.name().as_bytes().to_vec()),
            _ => exec_err!("Unknown property: {}", property),
        }
    }

    fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        self.plan.execute(partition, self.task_context.clone())
    }
}

impl From<ExportedExecutionPlan> for SedonaCExecutionPlan {
    fn from(value: ExportedExecutionPlan) -> Self {
        let boxed = Box::new(value);
        Self {
            get_schema: Some(c_exec_plan_get_schema),
            get_property_schema: None,
            get_property: Some(c_exec_plan_get_property),
            with_property: None,
            execute: Some(c_exec_plan_execute),
            execute_async: None,
            reserved: null_mut(),
            release: Some(c_exec_plan_release),
            private_data: Box::into_raw(boxed) as *mut c_void,
        }
    }
}

unsafe extern "C" fn c_exec_plan_get_schema(
    self_: *const SedonaCExecutionPlan,
    out: *mut FFI_ArrowSchema,
) {
    let plan = &*((*self_).private_data as *const ExportedExecutionPlan);
    let schema = plan.schema();
    if let Ok(ffi_schema) = FFI_ArrowSchema::try_from(schema.as_ref()) {
        std::ptr::write(out, ffi_schema);
    }
}

unsafe extern "C" fn c_exec_plan_get_property(
    self_: *const SedonaCExecutionPlan,
    property: *const std::ffi::c_char,
    _args: *mut SedonaCExecutionPlanArgs,
    out: *mut arrow_array::ffi::FFI_ArrowArray,
    err: *mut SedonaCError,
) -> c_int {
    let plan = &*((*self_).private_data as *const ExportedExecutionPlan);
    let property_str = CStr::from_ptr(property).to_string_lossy();

    match plan.get_property(&property_str) {
        Ok(bytes) => {
            // Return the bytes as a single-element binary array
            use arrow_array::{builder::BinaryBuilder, Array};
            let mut builder = BinaryBuilder::new();
            builder.append_value(&bytes);
            let array = builder.finish();
            let ffi_array = arrow_array::ffi::FFI_ArrowArray::new(&array.to_data());
            std::ptr::write(out, ffi_array);
            ERRNO_OK
        }
        Err(e) => {
            *err = SedonaCError::new(&e.to_string());
            libc::EINVAL
        }
    }
}

unsafe extern "C" fn c_exec_plan_execute(
    self_: *const SedonaCExecutionPlan,
    args: *mut SedonaCExecutionPlanArgs,
    out: *mut FFI_ArrowArrayStream,
    err: *mut SedonaCError,
) -> c_int {
    let plan = &*((*self_).private_data as *const ExportedExecutionPlan);

    // Parse the execute args
    let args_ref = &*args;
    let args_slice = if args_ref.args.is_null() || args_ref.args_len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(args_ref.args, args_ref.args_len)
    };

    let execute_args: ExecuteArgs = match serde_json::from_slice(args_slice) {
        Ok(a) => a,
        Err(e) => {
            *err = SedonaCError::new(&format!("Failed to parse execute args: {}", e));
            return libc::EINVAL;
        }
    };

    match plan.execute(execute_args.partition) {
        Ok(stream) => {
            // Create a streaming reader that polls the stream lazily
            let reader = StreamingRecordBatchReader::new(stream, plan.runtime.clone());
            let ffi_stream = FFI_ArrowArrayStream::new(Box::new(reader));
            std::ptr::write(out, ffi_stream);
            ERRNO_OK
        }
        Err(e) => {
            *err = SedonaCError::new(&e.to_string());
            libc::EINVAL
        }
    }
}

unsafe extern "C" fn c_exec_plan_release(self_: *mut SedonaCExecutionPlan) {
    if !(*self_).private_data.is_null() {
        let _ = Box::from_raw((*self_).private_data as *mut ExportedExecutionPlan);
        (*self_).private_data = null_mut();
    }
    (*self_).release = None;
}

/// An [ExecutionPlan] that wraps an imported [SedonaCExecutionPlan].
///
/// This allows plans to be imported from across an FFI boundary and executed
/// within DataFusion.
pub struct ImportedSedonaCExec {
    inner: SedonaCExecutionPlan,
    schema: SchemaRef,
    properties: PlanProperties,
    supports_limit_pushdown: bool,
    name: String,
}

impl Debug for ImportedSedonaCExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Try to get debug string from the FFI plan
        if let Ok(debug_str) = self.get_debug_string() {
            f.debug_struct("ImportedTableProviderExec")
                .field("inner", &debug_str)
                .finish()
        } else {
            f.debug_struct("ImportedTableProviderExec").finish()
        }
    }
}

impl ImportedSedonaCExec {
    /// Create a new ImportedTableProviderExec from a SedonaCExecutionPlan.
    ///
    /// This will query the plan for its schema and properties.
    pub fn try_new(inner: SedonaCExecutionPlan) -> Result<Self> {
        // Get schema
        let Some(get_schema) = inner.get_schema else {
            return sedona_internal_err!("SedonaCExecutionPlan does not have get_schema");
        };

        let mut ffi_schema = FFI_ArrowSchema::empty();
        unsafe { get_schema(&inner, &mut ffi_schema) };
        let schema = Arc::new(Schema::try_from(&ffi_schema)?);

        // Get plan properties
        let props_args = PlanPropertiesArgs::from_ffi_plan(&inner)?;
        let supports_limit_pushdown = props_args.supports_limit_pushdown;
        let properties = props_args.into_plan_properties(schema.clone());

        // Get the inner plan's name
        let inner_name = get_plan_string_property(&inner, "name").unwrap_or_default();
        let name = format!("ImportedSedonaCExec<{}>", inner_name);

        Ok(Self {
            inner,
            schema,
            properties,
            supports_limit_pushdown,
            name,
        })
    }

    fn get_debug_string(&self) -> Result<String> {
        get_plan_string_property(&self.inner, "debug_string")
    }
}

impl DisplayAs for ImportedSedonaCExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let property = match t {
            DisplayFormatType::Default => "display_default",
            DisplayFormatType::Verbose => "display_verbose",
            DisplayFormatType::TreeRender => "display_tree_render",
        };

        if let Ok(display_str) = get_plan_string_property(&self.inner, property) {
            write!(f, "{}", display_str)
        } else {
            write!(f, "ImportedSedonaCExec")
        }
    }
}

impl ExecutionPlan for ImportedSedonaCExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Unknown
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.supports_limit_pushdown
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let Some(execute) = self.inner.execute else {
            return sedona_internal_err!("SedonaCExecutionPlan does not have execute");
        };

        let args = ExecuteArgs { partition };
        let args_bytes = serde_json::to_vec(&args).map_err(|e| {
            sedona_internal_datafusion_err!("Failed to serialize execute args: {}", e)
        })?;

        let mut ffi_args = SedonaCExecutionPlanArgs {
            args: args_bytes.as_ptr(),
            args_len: args_bytes.len(),
            exec_plans: std::ptr::null(),
            num_exec_plans: 0,
            exprs: std::ptr::null(),
            num_exprs: 0,
            reserved: null_mut(),
        };

        let mut ffi_stream = FFI_ArrowArrayStream::empty();
        let mut err = SedonaCError::default();

        let code = unsafe { execute(&self.inner, &mut ffi_args, &mut ffi_stream, &mut err) };

        if code != ERRNO_OK {
            return exec_err!("Failed to execute plan: {}", err);
        }

        // Convert FFI stream to SendableRecordBatchStream
        unsafe { ffi_stream_to_sendable(&mut ffi_stream) }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return exec_err!("ImportedTableProviderExec does not support children");
        }
        Ok(self)
    }
}


