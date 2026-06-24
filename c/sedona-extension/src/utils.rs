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

//! Utilities for converting between Arrow FFI streams and DataFusion streams.

use std::ffi::{c_int, CString};
use std::ptr::null_mut;
use std::sync::Mutex;

use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{ArrowError, DataType, Field, SchemaRef};
use datafusion_common::Result;
use datafusion_physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use sedona_common::{sedona_internal_datafusion_err, sedona_internal_err};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::extension::{
    SedonaCError, SedonaCExecutionPlan, SedonaCExecutionPlanArgs, SedonaCTableProvider,
};

/// Success return code for FFI functions.
pub const ERRNO_OK: c_int = 0;

/// Get the schema for a property from a [SedonaCExecutionPlan].
///
/// Returns the DataType describing the property's data type.
/// If `get_property_schema` is not implemented, defaults to Binary.
fn get_plan_property_data_type(plan: &SedonaCExecutionPlan, property: &str) -> Result<DataType> {
    let Some(get_property_schema) = plan.get_property_schema else {
        // Default to Binary if get_property_schema is not implemented
        return Ok(DataType::Binary);
    };

    let property_cstr = CString::new(property)
        .map_err(|e| sedona_internal_datafusion_err!("Invalid property name: {}", e))?;

    let mut ffi_schema = FFI_ArrowSchema::empty();
    let mut err = SedonaCError::default();

    let code =
        unsafe { get_property_schema(plan, property_cstr.as_ptr(), &mut ffi_schema, &mut err) };

    if code != ERRNO_OK {
        return sedona_internal_err!("Failed to get property schema for '{}': {}", property, err);
    }

    // Try to convert the FFI schema to a Field
    let field = Field::try_from(&ffi_schema).map_err(|e| {
        sedona_internal_datafusion_err!("Failed to parse property schema for '{}': {}", property, e)
    })?;
    Ok(field.data_type().clone())
}

/// Call `get_property` on a [SedonaCExecutionPlan] and deserialize the result.
///
/// This handles the common pattern of:
/// 1. Extracting the get_property function pointer
/// 2. Calling it with a property name and optional serializable args
/// 3. Parsing the binary array result
/// 4. Deserializing from JSON to the target type
///
/// # Arguments
///
/// * `plan` - The execution plan to query
/// * `property` - The property name to retrieve
/// * `args` - Optional arguments to pass (will be serialized to JSON bytes)
///
/// # Errors
///
/// Returns an error if:
/// - The plan does not have a `get_property` callback
/// - The FFI call fails
/// - The result cannot be parsed as a binary array
/// - The JSON deserialization fails
pub fn get_plan_property<T, A>(
    plan: &SedonaCExecutionPlan,
    property: &str,
    args: Option<&A>,
) -> Result<T>
where
    T: DeserializeOwned,
    A: Serialize,
{
    let Some(get_property) = plan.get_property else {
        return sedona_internal_err!("SedonaCExecutionPlan does not have get_property");
    };

    let property_cstr = CString::new(property)
        .map_err(|e| sedona_internal_datafusion_err!("Invalid property name: {}", e))?;

    // Serialize args if provided
    let args_bytes = match args {
        Some(a) => serde_json::to_vec(a)
            .map_err(|e| sedona_internal_datafusion_err!("Failed to serialize args: {}", e))?,
        None => Vec::new(),
    };

    let mut ffi_args = SedonaCExecutionPlanArgs {
        args: if args_bytes.is_empty() {
            std::ptr::null()
        } else {
            args_bytes.as_ptr()
        },
        args_len: args_bytes.len(),
        exec_plans: std::ptr::null(),
        num_exec_plans: 0,
        exprs: std::ptr::null(),
        num_exprs: 0,
        reserved: null_mut(),
    };

    let mut ffi_array = arrow_array::ffi::FFI_ArrowArray::empty();
    let mut err = SedonaCError::default();

    let code = unsafe {
        get_property(
            plan,
            property_cstr.as_ptr(),
            &mut ffi_args,
            &mut ffi_array,
            &mut err,
        )
    };

    if code != ERRNO_OK {
        return sedona_internal_err!("Failed to get property '{}': {}", property, err);
    }

    // Get the property schema to know how to interpret the array
    let data_type = get_plan_property_data_type(plan, property)?;

    // Parse the array to get the JSON bytes
    parse_ffi_array(ffi_array, &data_type)
}

/// Parse an FFI array containing JSON and deserialize to the target type.
fn parse_ffi_array<T: DeserializeOwned>(
    ffi_array: arrow_array::ffi::FFI_ArrowArray,
    data_type: &DataType,
) -> Result<T> {
    let bytes = parse_ffi_array_to_bytes(ffi_array, data_type)?;
    serde_json::from_slice::<T>(&bytes)
        .map_err(|e| sedona_internal_datafusion_err!("Failed to deserialize property: {}", e))
}

/// Parse an FFI array and return the raw bytes.
fn parse_ffi_array_to_bytes(
    ffi_array: arrow_array::ffi::FFI_ArrowArray,
    data_type: &DataType,
) -> Result<Vec<u8>> {
    let data = unsafe { arrow_array::ffi::from_ffi_and_data_type(ffi_array, data_type.clone())? };
    let array = arrow_array::make_array(data);

    // Handle different array types
    match data_type {
        DataType::Binary => {
            let binary_array = array
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .ok_or_else(|| {
                    sedona_internal_datafusion_err!("Expected binary array from get_property")
                })?;
            Ok(binary_array.value(0).to_vec())
        }
        DataType::LargeBinary => {
            let binary_array = array
                .as_any()
                .downcast_ref::<arrow_array::LargeBinaryArray>()
                .ok_or_else(|| {
                    sedona_internal_datafusion_err!("Expected large binary array from get_property")
                })?;
            Ok(binary_array.value(0).to_vec())
        }
        DataType::Utf8 => {
            let string_array = array
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| {
                    sedona_internal_datafusion_err!("Expected string array from get_property")
                })?;
            Ok(string_array.value(0).as_bytes().to_vec())
        }
        DataType::LargeUtf8 => {
            let string_array = array
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
                .ok_or_else(|| {
                    sedona_internal_datafusion_err!("Expected large string array from get_property")
                })?;
            Ok(string_array.value(0).as_bytes().to_vec())
        }
        _ => sedona_internal_err!("Unsupported data type for property: {:?}", data_type),
    }
}

/// Get a string property from a [SedonaCExecutionPlan].
///
/// This is a convenience wrapper around [get_plan_property] for string values.
pub fn get_plan_string_property(plan: &SedonaCExecutionPlan, property: &str) -> Result<String> {
    let Some(get_property) = plan.get_property else {
        return sedona_internal_err!("SedonaCExecutionPlan does not have get_property");
    };

    let property_cstr = CString::new(property)
        .map_err(|e| sedona_internal_datafusion_err!("Invalid property name: {}", e))?;

    let mut ffi_args = SedonaCExecutionPlanArgs {
        args: std::ptr::null(),
        args_len: 0,
        exec_plans: std::ptr::null(),
        num_exec_plans: 0,
        exprs: std::ptr::null(),
        num_exprs: 0,
        reserved: null_mut(),
    };

    let mut ffi_array = arrow_array::ffi::FFI_ArrowArray::empty();
    let mut err = SedonaCError::default();

    let code = unsafe {
        get_property(
            plan,
            property_cstr.as_ptr(),
            &mut ffi_args,
            &mut ffi_array,
            &mut err,
        )
    };

    if code != ERRNO_OK {
        return sedona_internal_err!("Failed to get '{}': {}", property, err);
    }

    // Get the property schema to know how to interpret the array
    let data_type = get_plan_property_data_type(plan, property)?;

    let bytes = parse_ffi_array_to_bytes(ffi_array, &data_type)?;
    String::from_utf8(bytes)
        .map_err(|e| sedona_internal_datafusion_err!("Invalid UTF-8 in '{}': {}", property, e))
}

/// Get the schema for a property from a [SedonaCTableProvider].
///
/// Returns the DataType describing the property's data type.
/// If `get_property_schema` is not implemented, defaults to Binary.
fn get_table_provider_property_data_type(
    provider: &SedonaCTableProvider,
    property: &str,
) -> Result<DataType> {
    let Some(get_property_schema) = provider.get_property_schema else {
        // Default to Binary if get_property_schema is not implemented
        return Ok(DataType::Binary);
    };

    let property_cstr = CString::new(property)
        .map_err(|e| sedona_internal_datafusion_err!("Invalid property name: {}", e))?;

    let mut ffi_schema = FFI_ArrowSchema::empty();
    let mut err = SedonaCError::default();

    let code =
        unsafe { get_property_schema(provider, property_cstr.as_ptr(), &mut ffi_schema, &mut err) };

    if code != ERRNO_OK {
        return sedona_internal_err!("Failed to get property schema for '{}': {}", property, err);
    }

    // Try to convert the FFI schema to a Field
    let field = Field::try_from(&ffi_schema).map_err(|e| {
        sedona_internal_datafusion_err!("Failed to parse property schema for '{}': {}", property, e)
    })?;
    Ok(field.data_type().clone())
}

/// Get a string property from a [SedonaCTableProvider].
pub fn get_table_provider_string_property(
    provider: &SedonaCTableProvider,
    property: &str,
) -> Result<String> {
    let Some(get_property) = provider.get_property else {
        return sedona_internal_err!("SedonaCTableProvider does not have get_property");
    };

    let property_cstr = CString::new(property)
        .map_err(|e| sedona_internal_datafusion_err!("Invalid property name: {}", e))?;

    let mut ffi_args = SedonaCExecutionPlanArgs {
        args: std::ptr::null(),
        args_len: 0,
        exec_plans: std::ptr::null(),
        num_exec_plans: 0,
        exprs: std::ptr::null(),
        num_exprs: 0,
        reserved: null_mut(),
    };

    let mut ffi_array = arrow_array::ffi::FFI_ArrowArray::empty();
    let mut err = SedonaCError::default();

    let code = unsafe {
        get_property(
            provider,
            property_cstr.as_ptr(),
            &mut ffi_args,
            &mut ffi_array,
            &mut err,
        )
    };

    if code != ERRNO_OK {
        return sedona_internal_err!("Failed to get '{}': {}", property, err);
    }

    // Get the property schema to know how to interpret the array
    let data_type = get_table_provider_property_data_type(provider, property)?;

    let bytes = parse_ffi_array_to_bytes(ffi_array, &data_type)?;
    String::from_utf8(bytes)
        .map_err(|e| sedona_internal_datafusion_err!("Invalid UTF-8 in '{}': {}", property, e))
}

/// A RecordBatchReader that lazily polls a SendableRecordBatchStream.
///
/// This allows exporting a DataFusion stream over FFI without collecting all
/// batches upfront. Used when exporting execution plans across FFI boundaries.
pub struct StreamingRecordBatchReader {
    schema: SchemaRef,
    stream: Mutex<SendableRecordBatchStream>,
    runtime: tokio::runtime::Handle,
}

impl StreamingRecordBatchReader {
    /// Create a new StreamingRecordBatchReader from a SendableRecordBatchStream.
    pub fn new(stream: SendableRecordBatchStream, runtime: tokio::runtime::Handle) -> Self {
        Self {
            schema: stream.schema(),
            stream: Mutex::new(stream),
            runtime,
        }
    }
}

impl Iterator for StreamingRecordBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let stream = &self.stream;
        let runtime = &self.runtime;

        std::thread::scope(|s| {
            s.spawn(|| {
                let mut guard = stream.lock().unwrap();
                runtime.block_on(async {
                    match guard.next().await {
                        Some(Ok(batch)) => Some(Ok(batch)),
                        Some(Err(e)) => Some(Err(ArrowError::ExternalError(Box::new(e)))),
                        None => None,
                    }
                })
            })
            .join()
            .unwrap_or_else(|_| {
                Some(Err(ArrowError::InvalidArgumentError(
                    "Iterator thread panicked".to_string(),
                )))
            })
        })
    }
}

impl RecordBatchReader for StreamingRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Convert an FFI ArrowArrayStream into a SendableRecordBatchStream.
///
/// This is the inverse of StreamingRecordBatchReader - it takes an FFI stream
/// and converts it back to a DataFusion stream. Used when importing execution
/// plans from across FFI boundaries.
///
/// # Safety
///
/// The caller must ensure that the FFI stream pointer is valid and properly
/// initialized.
pub unsafe fn ffi_stream_to_sendable(
    ffi_stream: &mut FFI_ArrowArrayStream,
) -> Result<SendableRecordBatchStream> {
    let reader = arrow_array::ffi_stream::ArrowArrayStreamReader::from_raw(ffi_stream)?;

    let schema = reader.schema();
    let stream = futures::stream::iter(reader).map(|result| {
        result.map_err(|e| datafusion_common::DataFusionError::ArrowError(Box::new(e), None))
    });

    Ok(Box::pin(
        datafusion_physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream),
    ))
}
