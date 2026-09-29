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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_plan::ExecutionPlan;
use pyo3::{
    pyclass, pyfunction, pymethods,
    types::{PyAnyMethods, PyCapsule},
    Bound, Py, PyAny, Python,
};
use sedona_catalog::{
    SedonaCatalog, SedonaCatalogList, SedonaCatalogRef, SedonaSchema, SedonaSchemaRef,
};
use sedona_extension::{
    execution_plan::{ExportedExecutionPlan, ImportedSedonaCExec},
    extension::SedonaCExecutionPlan,
    runtime::RuntimeHandle,
};

use crate::error::PySedonaError;
use crate::import_from::{check_pycapsule, import_table_provider_from_any};

#[derive(Clone)]
struct CatalogContext {
    task_context: Arc<TaskContext>,
    runtime: Arc<RuntimeHandle>,
}

/// Python-backed top-level catalog list.
pub struct PySedonaCatalogList {
    object: Py<PyAny>,
    context: CatalogContext,
}

impl PySedonaCatalogList {
    pub fn new(
        object: Py<PyAny>,
        task_context: Arc<TaskContext>,
        runtime: Arc<RuntimeHandle>,
    ) -> Self {
        Self {
            object,
            context: CatalogContext {
                task_context,
                runtime,
            },
        }
    }
}

impl Debug for PySedonaCatalogList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PySedonaCatalogList").finish()
    }
}

impl SedonaCatalogList for PySedonaCatalogList {
    fn catalog_names(&self) -> Vec<String> {
        Python::attach(|py| {
            self.object
                .call_method0(py, "catalog_names")
                .and_then(|value| value.extract(py))
                .unwrap_or_default()
        })
    }

    fn catalog(&self, name: &str) -> Option<SedonaCatalogRef> {
        Python::attach(|py| {
            let value = self.object.call_method1(py, "catalog", (name,)).ok()?;
            if value.is_none(py) {
                None
            } else {
                Some(Arc::new(PySedonaCatalog {
                    object: value,
                    context: self.context.clone(),
                }) as _)
            }
        })
    }

    fn create(&self, name: &str) -> Result<SedonaCatalogRef> {
        Python::attach(|py| {
            let value = self
                .object
                .call_method1(py, "create", (name,))
                .map_err(|error| py_error("catalog create", error))?;
            if value.is_none(py) {
                return Err(DataFusionError::External(
                    "Python catalog create() returned None".into(),
                ));
            }
            Ok(Arc::new(PySedonaCatalog {
                object: value,
                context: self.context.clone(),
            }) as _)
        })
    }
}

struct PySedonaCatalog {
    object: Py<PyAny>,
    context: CatalogContext,
}

impl Debug for PySedonaCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PySedonaCatalog").finish()
    }
}

impl SedonaCatalog for PySedonaCatalog {
    fn schema_names(&self) -> Vec<String> {
        Python::attach(|py| {
            self.object
                .call_method0(py, "schema_names")
                .and_then(|value| value.extract(py))
                .unwrap_or_default()
        })
    }

    fn schema(&self, name: &str) -> Option<SedonaSchemaRef> {
        Python::attach(|py| {
            let value = self.object.call_method1(py, "schema", (name,)).ok()?;
            if value.is_none(py) {
                None
            } else {
                PySedonaSchema::try_new(py, value, self.context.clone())
                    .ok()
                    .map(|schema| Arc::new(schema) as _)
            }
        })
    }

    fn create(&self, name: &str) -> Result<SedonaSchemaRef> {
        Python::attach(|py| {
            let value = self
                .object
                .call_method1(py, "create", (name,))
                .map_err(|error| py_error("schema create", error))?;
            if value.is_none(py) {
                return Err(DataFusionError::External(
                    "Python schema create() returned None".into(),
                ));
            }
            Ok(Arc::new(PySedonaSchema::try_new(py, value, self.context.clone())?) as _)
        })
    }

    fn deregister(&self, name: &str, cascade: bool) -> Result<Option<SedonaSchemaRef>> {
        Python::attach(|py| {
            let value = self
                .object
                .call_method1(py, "deregister", (name, cascade))
                .map_err(|error| py_error("schema deregister", error))?;
            if value.is_none(py) {
                Ok(None)
            } else {
                Ok(Some(
                    Arc::new(PySedonaSchema::try_new(py, value, self.context.clone())?) as _,
                ))
            }
        })
    }
}

struct PySedonaSchema {
    object: Py<PyAny>,
    owner_name: Option<String>,
    context: CatalogContext,
}

impl PySedonaSchema {
    fn try_new(py: Python<'_>, object: Py<PyAny>, context: CatalogContext) -> Result<Self> {
        let owner_name = object
            .call_method0(py, "owner_name")
            .and_then(|value| value.extract(py))
            .map_err(|error| py_error("schema owner_name", error))?;
        Ok(Self {
            object,
            owner_name,
            context,
        })
    }
}

impl Debug for PySedonaSchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PySedonaSchema").finish()
    }
}

#[async_trait]
impl SedonaSchema for PySedonaSchema {
    fn owner_name(&self) -> Option<&str> {
        self.owner_name.as_deref()
    }

    fn table_names(&self) -> Vec<String> {
        Python::attach(|py| {
            self.object
                .call_method0(py, "table_names")
                .and_then(|value| value.extract(py))
                .unwrap_or_default()
        })
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let object = Python::attach(|py| self.object.clone_ref(py));
        let name = name.to_string();
        tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                let value = object
                    .call_method1(py, "table", (name,))
                    .map_err(|error| py_error("table lookup", error))?;
                if value.is_none(py) {
                    Ok(None)
                } else {
                    let (provider, _) =
                        import_table_provider_from_any(py, value.bind(py), None, true)
                            .map_err(|error| py_error("table import", error))?;
                    Ok(Some(provider))
                }
            })
        })
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))?
    }

    fn create(&self, name: &str, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        Python::attach(|py| {
            let input = Bound::new(
                py,
                PyExecutionPlan {
                    plan: input,
                    context: self.context.clone(),
                },
            )
            .map_err(|error| py_error("execution plan wrapper", error))?;
            let value = self
                .object
                .call_method1(py, "create", (name, input))
                .map_err(|error| py_error("table create", error))?;
            import_execution_plan(py, value.bind(py))
        })
    }

    fn deregister(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Python::attach(|py| {
            let value = self
                .object
                .call_method1(py, "deregister", (name,))
                .map_err(|error| py_error("table deregister", error))?;
            if value.is_none(py) {
                Ok(None)
            } else {
                let (provider, _) = import_table_provider_from_any(py, value.bind(py), None, true)
                    .map_err(|error| py_error("table import", error))?;
                Ok(Some(provider))
            }
        })
    }

    fn table_exist(&self, name: &str) -> bool {
        Python::attach(|py| {
            self.object
                .call_method1(py, "table_exist", (name,))
                .and_then(|value| value.extract(py))
                .unwrap_or(false)
        })
    }
}

/// Opaque physical plan passed to Python `Schema.create()` implementations.
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct PyExecutionPlan {
    plan: Arc<dyn ExecutionPlan>,
    context: CatalogContext,
}

#[pymethods]
impl PyExecutionPlan {
    #[getter]
    fn name(&self) -> String {
        self.plan.name().to_string()
    }

    fn __sedonadb_execution_plan__<'py>(
        &self,
        py: Python<'py>,
    ) -> Result<Bound<'py, PyCapsule>, PySedonaError> {
        let exported = ExportedExecutionPlan::new(
            self.plan.clone(),
            self.context.task_context.clone(),
            self.context.runtime.clone(),
        );
        Ok(PyCapsule::new_with_value(
            py,
            SedonaCExecutionPlan::from(exported),
            c"sedonadb_execution_plan",
        )?)
    }

    fn __repr__(&self) -> String {
        format!("PyExecutionPlan(name={:?})", self.plan.name())
    }
}

fn import_execution_plan(
    _py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Ok(wrapper) = value.extract::<PyExecutionPlan>() {
        return Ok(wrapper.plan);
    }
    if value
        .hasattr("__sedonadb_execution_plan__")
        .unwrap_or(false)
    {
        let capsule = value
            .call_method0("__sedonadb_execution_plan__")
            .map_err(|error| py_error("execution plan export", error))?;
        let contents = check_pycapsule(&capsule, "sedonadb_execution_plan")
            .map_err(|error| py_error("execution plan import", error))?
            as *mut SedonaCExecutionPlan;
        let plan = unsafe {
            let plan = std::ptr::read(contents);
            std::ptr::write_bytes(contents, 0, 1);
            plan
        };
        return Ok(Arc::new(ImportedSedonaCExec::try_new(plan)?));
    }
    Err(DataFusionError::External(
        "Python Schema.create() must return a PyExecutionPlan or an object implementing \
         __sedonadb_execution_plan__"
            .into(),
    ))
}

fn py_error(context: &str, error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::External(format!("Python catalog {context} error: {error}").into())
}

/// Hold a Python catalog list until it is attached to a SedonaDB context.
#[pyclass]
pub struct PyCatalogListWrapper {
    pub object: Py<PyAny>,
}

#[pymethods]
impl PyCatalogListWrapper {
    fn __repr__(&self) -> String {
        "PyCatalogListWrapper()".to_string()
    }
}

#[pyfunction]
pub fn py_catalog_list(object: Py<PyAny>) -> PyCatalogListWrapper {
    PyCatalogListWrapper { object }
}
