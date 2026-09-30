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
use serde::{Deserialize, Serialize};

pub use adapter::{DataFusionCatalog, DataFusionCatalogList, DataFusionSchema};

/// The behavior to use when creating an object that already exists.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CreateMode {
    /// Create the object and return an error if it already exists.
    #[default]
    Create,
    /// Create the object unless it already exists.
    CreateOrIgnore,
    /// Create the object, replacing an existing object with the same name.
    Replace,
}

/// Options for creating a catalog.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CreateCatalogOptions {
    /// The behavior to use when a catalog with the same name already exists.
    pub mode: CreateMode,
}

/// Options for creating a schema.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CreateSchemaOptions {
    /// The behavior to use when a schema with the same name already exists.
    pub mode: CreateMode,
}

/// Options for creating a table.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CreateTableOptions {
    /// The behavior to use when a table with the same name already exists.
    pub mode: CreateMode,
    /// Whether the table is temporary.
    pub temporary: bool,
}

/// Options for dropping a schema.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DropSchemaOptions {
    /// Whether objects contained by the schema should also be dropped.
    pub cascade: bool,
}

/// The kind of catalog object targeted by a table-like operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogObjectType {
    /// A physical table.
    Table,
    /// A non-materialized view.
    View,
}

/// Options for dropping a table.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DropTableOptions {
    /// The expected kind of object, or `None` when the caller cannot distinguish it.
    ///
    /// Implementations should not drop an object whose kind does not match.
    pub object_type: Option<CatalogObjectType>,
    /// Whether data and metadata referenced by a table should also be deleted.
    /// This option does not apply to views.
    pub purge: bool,
}

/// A collection of catalogs backed by a SedonaDB extension.
pub trait SedonaCatalogList: Debug + Send + Sync {
    /// Return the names of the catalogs in this list.
    fn catalog_names(&self) -> Vec<String>;

    /// Return the catalog named `name`, or `None` when it does not exist.
    fn catalog(&self, name: &str) -> Option<Arc<dyn SedonaCatalog>>;

    /// Create a catalog in the backing catalog system.
    fn create(&self, name: &str, options: &CreateCatalogOptions) -> Result<Arc<dyn SedonaCatalog>>;
}

/// A collection of schemas backed by a SedonaDB extension.
pub trait SedonaCatalog: Debug + Send + Sync {
    /// Return the names of the schemas in this catalog.
    fn schema_names(&self) -> Vec<String>;

    /// Return the schema named `name`, or `None` when it does not exist.
    fn schema(&self, name: &str) -> Option<Arc<dyn SedonaSchema>>;

    /// Create a schema in the backing catalog system.
    fn create(&self, name: &str, options: &CreateSchemaOptions) -> Result<Arc<dyn SedonaSchema>>;

    /// Drop a schema from the backing catalog system.
    ///
    /// Returns the dropped schema when it existed, or `None` otherwise.
    fn drop_schema(
        &self,
        name: &str,
        options: &DropSchemaOptions,
    ) -> Result<Option<Arc<dyn SedonaSchema>>>;
}

/// A collection of tables backed by a SedonaDB extension.
#[async_trait]
pub trait SedonaSchema: Debug + Send + Sync {
    /// Return the name of the schema owner, when one is available.
    fn owner_name(&self) -> Option<&str> {
        None
    }

    /// Return the names of the tables in this schema.
    fn table_names(&self) -> Vec<String>;

    /// Return the table named `name`, or `None` when it does not exist.
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>>;

    /// Create a table from a physical input plan.
    ///
    /// The returned plan performs the create operation when it is executed.
    fn create(
        &self,
        name: &str,
        options: &CreateTableOptions,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Drop a table from the backing catalog system.
    ///
    /// Returns the dropped table when it existed, or `None` otherwise.
    fn drop_table(
        &self,
        name: &str,
        options: &DropTableOptions,
    ) -> Result<Option<Arc<dyn TableProvider>>>;

    /// Return whether a table named `name` exists in this schema.
    fn table_exist(&self, name: &str) -> bool;
}
