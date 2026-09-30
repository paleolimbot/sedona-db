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
    collections::HashSet,
    sync::{Arc, Weak},
};

use crate::object_storage::ensure_object_store_registered;

use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};

use async_trait::async_trait;
use datafusion::common::plan_datafusion_err;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use parking_lot::RwLock;
use sedona_catalog::{CreateCatalogOptions, DataFusionCatalog, SedonaCatalog, SedonaCatalogList};

/// Owns the catalog composition for a Sedona session.
///
/// Foreign Sedona catalogs are searched newest-first and take precedence over
/// DataFusion's built-in catalogs. Ordinary DataFusion registration continues
/// to target the built-in list. When enabled, built-in schemas are wrapped to
/// register object stores dynamically for file locations.
#[derive(Debug)]
pub struct SedonaCatalogRegistry {
    foreign: RwLock<Vec<Arc<dyn SedonaCatalogList>>>,
    fallback: Arc<dyn CatalogProviderList>,
    state: Weak<RwLock<SessionState>>,
    dynamic_object_store: bool,
}

impl SedonaCatalogRegistry {
    /// Create a registry that layers foreign catalogs over `fallback`.
    pub fn new(
        fallback: Arc<dyn CatalogProviderList>,
        state: Weak<RwLock<SessionState>>,
        dynamic_object_store: bool,
    ) -> Self {
        Self {
            foreign: RwLock::new(Vec::new()),
            fallback,
            state,
            dynamic_object_store,
        }
    }

    /// Add a foreign catalog list. Later registrations take precedence.
    pub fn register_foreign(&self, catalogs: Arc<dyn SedonaCatalogList>) {
        self.foreign.write().push(catalogs);
    }

    /// Find a catalog owned by a foreign catalog list.
    pub fn foreign_catalog(&self, name: &str) -> Result<Option<Arc<dyn SedonaCatalog>>> {
        let foreign = self.foreign.read().clone();
        for catalogs in foreign.iter().rev() {
            if let Some(catalog) = catalogs.catalog(name)? {
                return Ok(Some(catalog));
            }
        }
        Ok(None)
    }

    /// Return whether at least one foreign catalog list is registered.
    pub fn has_foreign_catalog_list(&self) -> bool {
        !self.foreign.read().is_empty()
    }

    /// Return whether either a foreign or built-in catalog has this name.
    pub fn catalog_exists(&self, name: &str) -> Result<bool> {
        Ok(self.foreign_catalog(name)?.is_some() || self.fallback.catalog(name).is_some())
    }

    /// Create a catalog in the most recently registered foreign catalog list.
    ///
    /// `None` means that no foreign catalog list is installed and the caller
    /// should delegate the operation to DataFusion.
    pub fn create_foreign_catalog(
        &self,
        name: &str,
        options: &CreateCatalogOptions,
    ) -> Option<Result<Arc<dyn SedonaCatalog>>> {
        let catalogs = self.foreign.read().last().cloned()?;
        Some(catalogs.create(name, options))
    }
}

impl CatalogProviderList for SedonaCatalogRegistry {
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.fallback.register_catalog(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        self.foreign
            .read()
            .iter()
            .rev()
            .flat_map(|catalogs| catalogs.catalog_names().unwrap_or_default())
            .chain(self.fallback.catalog_names())
            .filter(|name| seen.insert(name.clone()))
            .collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        match self.foreign_catalog(name) {
            Ok(Some(catalog)) => return Some(Arc::new(DataFusionCatalog::new(catalog))),
            Ok(None) => {}
            Err(error) => {
                return Some(Arc::new(DataFusionCatalog::new_error(error.to_string())));
            }
        }

        let catalog = self.fallback.catalog(name)?;
        if !self.dynamic_object_store {
            return Some(catalog);
        }

        let state = self.state.clone();
        Some(Arc::new(DynamicObjectStoreCatalogProvider::new(
            catalog, state,
        )))
    }
}

/// Wraps another catalog provider
#[derive(Debug)]
struct DynamicObjectStoreCatalogProvider {
    inner: Arc<dyn CatalogProvider>,
    state: Weak<RwLock<SessionState>>,
}

impl DynamicObjectStoreCatalogProvider {
    pub fn new(inner: Arc<dyn CatalogProvider>, state: Weak<RwLock<SessionState>>) -> Self {
        Self { inner, state }
    }
}

impl CatalogProvider for DynamicObjectStoreCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let state = self.state.clone();
        self.inner
            .schema(name)
            .map(|schema| Arc::new(DynamicObjectStoreSchemaProvider::new(schema, state)) as _)
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        self.inner.register_schema(name, schema)
    }
}

/// Wraps another schema provider. [DynamicObjectStoreSchemaProvider] is responsible for registering the required
/// object stores for the file locations.
#[derive(Debug)]
struct DynamicObjectStoreSchemaProvider {
    inner: Arc<dyn SchemaProvider>,
    state: Weak<RwLock<SessionState>>,
}

impl DynamicObjectStoreSchemaProvider {
    pub fn new(inner: Arc<dyn SchemaProvider>, state: Weak<RwLock<SessionState>>) -> Self {
        Self { inner, state }
    }
}

#[async_trait]
impl SchemaProvider for DynamicObjectStoreSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let inner_table = self.inner.table(name).await;
        if inner_table.is_ok() {
            if let Some(inner_table) = inner_table? {
                return Ok(Some(inner_table));
            }
        }

        // if the inner schema provider didn't have a table by
        // that name, try to treat it as a listing table
        let mut state = self
            .state
            .upgrade()
            .ok_or_else(|| plan_datafusion_err!("locking error"))?
            .read()
            .clone();

        ensure_object_store_registered(&mut state, name).await?;
        self.inner.table(name).await
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}

#[cfg(test)]
mod tests {

    use crate::context::SedonaContext;

    use super::*;

    use datafusion::{catalog::SchemaProvider, datasource::listing::ListingTableUrl};
    use sedona_catalog::{CreateCatalogOptions, SedonaCatalogList};

    #[derive(Debug)]
    struct FailingCatalogList;

    impl SedonaCatalogList for FailingCatalogList {
        fn catalog_names(&self) -> Result<Vec<String>> {
            Ok(vec!["datafusion".to_owned()])
        }

        fn catalog(&self, _name: &str) -> Result<Option<Arc<dyn SedonaCatalog>>> {
            datafusion_common::exec_err!("foreign catalog lookup failed")
        }

        fn create(
            &self,
            _name: &str,
            _options: &CreateCatalogOptions,
        ) -> Result<Arc<dyn SedonaCatalog>> {
            datafusion_common::not_impl_err!("not needed by lookup test")
        }
    }

    fn setup_context() -> (SedonaContext, Arc<dyn SchemaProvider>) {
        let ctx = SedonaContext::new();
        ctx.ctx
            .register_catalog_list(Arc::new(SedonaCatalogRegistry::new(
                ctx.ctx.state().catalog_list().clone(),
                ctx.ctx.state_weak_ref(),
                true,
            )));

        let provider = &SedonaCatalogRegistry::new(
            ctx.ctx.state().catalog_list().clone(),
            ctx.ctx.state_weak_ref(),
            true,
        ) as &dyn CatalogProviderList;
        let catalog = provider
            .catalog(provider.catalog_names().first().unwrap())
            .unwrap();
        let schema = catalog
            .schema(catalog.schema_names().first().unwrap())
            .unwrap();
        (ctx, schema)
    }

    #[tokio::test]
    async fn query_http_location_test() -> Result<()> {
        // This is a unit test so not expecting a connection or a file to be
        // available
        let domain = "example.com";
        let location = format!("http://{domain}/file.parquet");

        let (ctx, schema) = setup_context();

        // That's a non registered table so expecting None here
        let table = schema.table(&location).await?;
        assert!(table.is_none());

        // It should still create an object store for the location in the SessionState
        let store = ctx
            .ctx
            .runtime_env()
            .object_store(ListingTableUrl::parse(location)?)?;

        assert_eq!(format!("{store}"), "HttpStore");

        // The store must be configured for this domain
        let expected_domain = format!("Domain(\"{domain}\")");
        assert!(format!("{store:?}").contains(&expected_domain));

        Ok(())
    }

    #[tokio::test]
    async fn foreign_lookup_error_does_not_fall_back_to_builtin_catalog() -> Result<()> {
        let ctx = SedonaContext::new();
        ctx.register_catalog_list(Arc::new(FailingCatalogList));

        let catalog = ctx.ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();
        let error = schema.table("any_table").await.unwrap_err();
        assert!(error.to_string().contains("foreign catalog lookup failed"));

        Ok(())
    }

    #[tokio::test]
    async fn query_s3_location_test() -> Result<()> {
        let bucket = "examples3bucket";
        let location = format!("s3://{bucket}/file.parquet");

        let (ctx, schema) = setup_context();

        let table = schema.table(&location).await?;
        assert!(table.is_none());

        let store = ctx
            .ctx
            .runtime_env()
            .object_store(ListingTableUrl::parse(location)?)?;
        assert_eq!(format!("{store}"), format!("AmazonS3({bucket})"));

        // The store must be configured for this domain
        let expected_bucket = format!("bucket: \"{bucket}\"");
        assert!(format!("{store:?}").contains(&expected_bucket));

        Ok(())
    }

    #[tokio::test]
    async fn query_gs_location_test() -> Result<()> {
        let bucket = "examplegsbucket";
        let location = format!("gs://{bucket}/file.parquet");

        let (ctx, schema) = setup_context();

        let table = schema.table(&location).await?;
        assert!(table.is_none());

        let store = ctx
            .ctx
            .runtime_env()
            .object_store(ListingTableUrl::parse(location)?)?;
        assert_eq!(format!("{store}"), format!("GoogleCloudStorage({bucket})"));

        // The store must be configured for this domain
        let expected_bucket = format!("bucket_name_encoded: \"{bucket}\"");
        assert!(format!("{store:?}").contains(&expected_bucket));

        Ok(())
    }

    #[tokio::test]
    async fn query_invalid_location_test() {
        let location = "ts://file.parquet";
        let (_ctx, schema) = setup_context();

        assert!(schema.table(location).await.is_err());
    }
}
