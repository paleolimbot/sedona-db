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

use crate::{SedonaCatalogListRef, SedonaCatalogRef, SedonaSchemaRef};

/// Exposes a [`crate::SedonaCatalogList`] to DataFusion's read-side catalog API.
pub struct DataFusionCatalogList {
    inner: SedonaCatalogListRef,
}

impl DataFusionCatalogList {
    pub fn new(inner: SedonaCatalogListRef) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &SedonaCatalogListRef {
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
    inner: SedonaCatalogRef,
}

impl DataFusionCatalog {
    pub fn new(inner: SedonaCatalogRef) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &SedonaCatalogRef {
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
        name: &str,
        cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self
            .inner
            .deregister(name, cascade)?
            .map(|schema| Arc::new(DataFusionSchema::new(schema)) as _))
    }
}

/// Exposes a [`crate::SedonaSchema`] to DataFusion's read-side catalog API.
pub struct DataFusionSchema {
    inner: SedonaSchemaRef,
}

impl DataFusionSchema {
    pub fn new(inner: SedonaSchemaRef) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &SedonaSchemaRef {
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

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}
