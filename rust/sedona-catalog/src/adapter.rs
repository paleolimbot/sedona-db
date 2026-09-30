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
use datafusion_common::{Result, not_impl_err};

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
        self.inner.catalog_names()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.inner
            .catalog(name)
            .map(|catalog| Arc::new(DataFusionCatalog::new(catalog)) as _)
    }
}

/// Exposes a [`crate::SedonaCatalog`] to DataFusion's read-side catalog API.
pub struct DataFusionCatalog {
    inner: Arc<dyn SedonaCatalog>,
}

impl DataFusionCatalog {
    /// Create an adapter over a Sedona catalog.
    pub fn new(inner: Arc<dyn SedonaCatalog>) -> Self {
        Self { inner }
    }

    /// Return the wrapped Sedona catalog.
    pub fn inner(&self) -> &Arc<dyn SedonaCatalog> {
        &self.inner
    }
}

impl Debug for DataFusionCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionCatalog")
            .field("inner", &self.inner)
            .finish()
    }
}

impl CatalogProvider for DataFusionCatalog {
    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.inner
            .schema(name)
            .map(|schema| Arc::new(DataFusionSchema::new(schema)) as _)
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

/// Exposes a [`crate::SedonaSchema`] to DataFusion's read-side catalog API.
pub struct DataFusionSchema {
    inner: Arc<dyn SedonaSchema>,
}

impl DataFusionSchema {
    /// Create an adapter over a Sedona schema.
    pub fn new(inner: Arc<dyn SedonaSchema>) -> Self {
        Self { inner }
    }

    /// Return the wrapped Sedona schema.
    pub fn inner(&self) -> &Arc<dyn SedonaSchema> {
        &self.inner
    }
}

impl Debug for DataFusionSchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionSchema")
            .field("inner", &self.inner)
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for DataFusionSchema {
    fn owner_name(&self) -> Option<&str> {
        self.inner.owner_name()
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.table(name).await
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
        self.inner.table_exist(name)
    }
}
