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
use datafusion_catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};
use datafusion_common::{Result, exec_err, not_impl_err};

use crate::{SedonaCatalog, SedonaCatalogList, SedonaSchema};

/// Exposes a [`crate::SedonaCatalogList`] to DataFusion's read-side catalog API.
pub struct DataFusionCatalogList {
    inner: Arc<dyn SedonaCatalogList>,
}

impl DataFusionCatalogList {
    /// Create an adapter over a Sedona catalog list.
    pub fn new(inner: Arc<dyn SedonaCatalogList>) -> Self {
        Self { inner }
    }

    /// Return the wrapped Sedona catalog list.
    pub fn inner(&self) -> &Arc<dyn SedonaCatalogList> {
        &self.inner
    }
}

impl Debug for DataFusionCatalogList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionCatalogList")
            .field("inner", &self.inner)
            .finish()
    }
}

impl CatalogProviderList for DataFusionCatalogList {
    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        // DataFusion has no error channel here, and passing a Rust-owned
        // catalog across the extension boundary is the ownership operation
        // this interface deliberately avoids. Call SedonaCatalogList::create.
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        // DataFusion has no error channel for listing catalogs. Named catalog
        // lookups preserve errors through an error-backed provider below.
        self.inner.catalog_names().unwrap_or_default()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        match self.inner.catalog(name) {
            Ok(Some(catalog)) => Some(Arc::new(DataFusionCatalog::new(catalog))),
            Ok(None) => None,
            Err(error) => Some(Arc::new(DataFusionCatalog::new_error(error.to_string()))),
        }
    }
}

enum DataFusionCatalogInner {
    Catalog(Arc<dyn SedonaCatalog>),
    Error(String),
}

/// Exposes a [`crate::SedonaCatalog`] to DataFusion's read-side catalog API.
pub struct DataFusionCatalog {
    inner: DataFusionCatalogInner,
}

impl DataFusionCatalog {
    /// Create an adapter over a Sedona catalog.
    pub fn new(inner: Arc<dyn SedonaCatalog>) -> Self {
        Self {
            inner: DataFusionCatalogInner::Catalog(inner),
        }
    }

    /// Return the wrapped Sedona catalog, or `None` for an error-backed adapter.
    pub fn inner(&self) -> Option<&Arc<dyn SedonaCatalog>> {
        match &self.inner {
            DataFusionCatalogInner::Catalog(inner) => Some(inner),
            DataFusionCatalogInner::Error(_) => None,
        }
    }

    /// Create an adapter that surfaces a failed catalog lookup from table access.
    pub fn new_error(error: impl Into<String>) -> Self {
        Self {
            inner: DataFusionCatalogInner::Error(error.into()),
        }
    }
}

impl Debug for DataFusionCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionCatalog")
            .field("inner", &self.inner())
            .finish()
    }
}

impl CatalogProvider for DataFusionCatalog {
    fn schema_names(&self) -> Vec<String> {
        match &self.inner {
            DataFusionCatalogInner::Catalog(inner) => inner.schema_names().unwrap_or_default(),
            DataFusionCatalogInner::Error(_) => Vec::new(),
        }
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        match &self.inner {
            DataFusionCatalogInner::Catalog(inner) => match inner.schema(name) {
                Ok(Some(schema)) => Some(Arc::new(DataFusionSchema::new(schema))),
                Ok(None) => None,
                Err(error) => Some(Arc::new(DataFusionSchema::new_error(error.to_string()))),
            },
            DataFusionCatalogInner::Error(error) => {
                Some(Arc::new(DataFusionSchema::new_error(error.clone())))
            }
        }
    }

    fn register_schema(
        &self,
        _name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        not_impl_err!(
            "Registering schemas is not supported by a Sedona catalog; use create instead"
        )
    }

    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        not_impl_err!(
            "Deregistering schemas is not supported by a Sedona catalog; use drop_schema instead"
        )
    }
}

enum DataFusionSchemaInner {
    Schema(Arc<dyn SedonaSchema>),
    Error(String),
}

/// Exposes a [`crate::SedonaSchema`] to DataFusion's read-side catalog API.
pub struct DataFusionSchema {
    inner: DataFusionSchemaInner,
}

impl DataFusionSchema {
    /// Create an adapter over a Sedona schema.
    pub fn new(inner: Arc<dyn SedonaSchema>) -> Self {
        Self {
            inner: DataFusionSchemaInner::Schema(inner),
        }
    }

    /// Return the wrapped Sedona schema, or `None` for an error-backed adapter.
    pub fn inner(&self) -> Option<&Arc<dyn SedonaSchema>> {
        match &self.inner {
            DataFusionSchemaInner::Schema(inner) => Some(inner),
            DataFusionSchemaInner::Error(_) => None,
        }
    }

    /// Create an adapter that returns a failed schema lookup from table access.
    pub fn new_error(error: impl Into<String>) -> Self {
        Self {
            inner: DataFusionSchemaInner::Error(error.into()),
        }
    }
}

impl Debug for DataFusionSchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionSchema")
            .field("inner", &self.inner())
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for DataFusionSchema {
    fn owner_name(&self) -> Option<&str> {
        match &self.inner {
            DataFusionSchemaInner::Schema(inner) => inner.owner_name().ok().flatten(),
            DataFusionSchemaInner::Error(_) => None,
        }
    }

    fn table_names(&self) -> Vec<String> {
        match &self.inner {
            DataFusionSchemaInner::Schema(inner) => inner.table_names().unwrap_or_default(),
            DataFusionSchemaInner::Error(_) => Vec::new(),
        }
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        match &self.inner {
            DataFusionSchemaInner::Schema(inner) => inner.table(name).await,
            DataFusionSchemaInner::Error(error) => {
                exec_err!("Foreign catalog lookup failed: {error}")
            }
        }
    }

    fn register_table(
        &self,
        _name: String,
        _table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        not_impl_err!("Registering tables is not supported by a Sedona schema; use create instead")
    }

    fn deregister_table(&self, _name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        not_impl_err!(
            "Deregistering tables is not supported by a Sedona schema; use drop_table instead"
        )
    }

    fn table_exist(&self, name: &str) -> bool {
        match &self.inner {
            DataFusionSchemaInner::Schema(inner) => inner.table_exist(name).unwrap_or(false),
            DataFusionSchemaInner::Error(_) => false,
        }
    }
}
