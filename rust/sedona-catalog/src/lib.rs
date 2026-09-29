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

//! Version-independent catalog interfaces used by SedonaDB extensions.
//!
//! DataFusion's catalog traits are designed around registering Rust objects.
//! That is not a useful ownership boundary for a foreign catalog: an Iceberg,
//! PostGIS, or Python implementation needs to *create* an object in its own
//! system. The traits in this crate mirror DataFusion's catalog hierarchy but
//! replace each `register_*` operation with [`SedonaCatalogList::create`],
//! [`SedonaCatalog::create`], or [`SedonaSchema::create`].

mod adapter;

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion_catalog::TableProvider;
use datafusion_common::Result;
use datafusion_physical_plan::ExecutionPlan;

pub use adapter::{DataFusionCatalog, DataFusionCatalogList, DataFusionSchema, OverlayCatalogList};

pub type SedonaCatalogListRef = Arc<dyn SedonaCatalogList>;
pub type SedonaCatalogRef = Arc<dyn SedonaCatalog>;
pub type SedonaSchemaRef = Arc<dyn SedonaSchema>;

/// A collection of catalogs backed by a SedonaDB extension.
pub trait SedonaCatalogList: Debug + Send + Sync {
    fn catalog_names(&self) -> Vec<String>;

    fn catalog(&self, name: &str) -> Option<SedonaCatalogRef>;

    /// Create a catalog in the backing catalog system.
    fn create(&self, name: &str) -> Result<SedonaCatalogRef>;
}

/// A collection of schemas backed by a SedonaDB extension.
pub trait SedonaCatalog: Debug + Send + Sync {
    fn schema_names(&self) -> Vec<String>;

    fn schema(&self, name: &str) -> Option<SedonaSchemaRef>;

    /// Create a schema in the backing catalog system.
    fn create(&self, name: &str) -> Result<SedonaSchemaRef>;

    fn deregister(&self, name: &str, cascade: bool) -> Result<Option<SedonaSchemaRef>>;
}

/// A collection of tables backed by a SedonaDB extension.
#[async_trait]
pub trait SedonaSchema: Debug + Send + Sync {
    fn owner_name(&self) -> Option<&str> {
        None
    }

    fn table_names(&self) -> Vec<String>;

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>>;

    /// Create a table from a physical input plan.
    ///
    /// The returned plan performs the create operation when it is executed.
    fn create(&self, name: &str, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>>;

    fn deregister(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>>;

    fn table_exist(&self, name: &str) -> bool;
}
