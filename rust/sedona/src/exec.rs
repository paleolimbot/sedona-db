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
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::prelude::DataFrame;
use datafusion_common::{exec_err, SchemaReference, TableReference};
use datafusion_expr::{DdlStatement, LogicalPlan, TableType};
use datafusion_physical_plan::ExecutionPlan;
use sedona_catalog::{
    CatalogObjectType, CreateCatalogOptions, CreateMode, CreateSchemaOptions, CreateTableOptions,
    DropSchemaOptions, DropTableOptions,
};
use std::fmt::Debug;
use std::sync::Arc;

use crate::{context::SedonaContext, object_storage::register_object_store_and_config_extensions};

use datafusion::{
    error::{DataFusionError, Result},
    sql::parser::Statement,
};

/// A Sedona-specific hook for creating a logical plan from a SQL Statement
///
/// Currently this provides support for CREATE EXTERNAL TABLE and
pub(crate) async fn create_plan_from_sql(
    ctx: &SedonaContext,
    statement: Statement,
) -> Result<LogicalPlan, DataFusionError> {
    let mut plan = ctx.ctx.state().statement_to_plan(statement).await?;

    // Note that cmd is a mutable reference so that create_external_table function can remove all
    // datafusion-cli specific options before passing through to datafusion. Otherwise, datafusion
    // will raise Configuration errors.
    if let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) = &plan {
        register_object_store_and_config_extensions(ctx, &cmd.location, &cmd.options).await?;
    }

    if let LogicalPlan::Copy(copy_to) = &mut plan {
        register_object_store_and_config_extensions(ctx, &copy_to.output_url, &copy_to.options)
            .await?;
    }

    Ok(plan)
}

/// Execute DDL against the Sedona-owned catalog interfaces when the target is
/// backed by a foreign catalog. Returning `None` delegates ordinary DDL to
/// DataFusion unchanged.
pub(crate) async fn execute_sedona_catalog_ddl(
    ctx: &SedonaContext,
    plan: &LogicalPlan,
) -> Result<Option<DataFrame>, DataFusionError> {
    let LogicalPlan::Ddl(ddl) = plan else {
        return Ok(None);
    };

    match ddl {
        DdlStatement::CreateCatalog(cmd) => {
            let catalogs = ctx.catalog_registry();
            if !catalogs.has_foreign_catalog_list() {
                return Ok(None);
            }
            if ctx.ctx.catalog(&cmd.catalog_name).is_some() {
                if cmd.if_not_exists {
                    return Ok(Some(ctx.ctx.read_empty()?));
                }
                return exec_err!("Catalog '{}' already exists", cmd.catalog_name);
            }
            catalogs
                .create_foreign_catalog(
                    &cmd.catalog_name,
                    &CreateCatalogOptions {
                        mode: if cmd.if_not_exists {
                            CreateMode::CreateOrIgnore
                        } else {
                            CreateMode::Create
                        },
                    },
                )
                .expect("foreign catalog list checked above")?;
            Ok(Some(ctx.ctx.read_empty()?))
        }
        DdlStatement::CreateCatalogSchema(cmd) => {
            let tokens: Vec<&str> = cmd.schema_name.split('.').collect();
            let (catalog_name, schema_name) = match tokens.as_slice() {
                [schema] => {
                    let state = ctx.ctx.state();
                    (
                        state.config().options().catalog.default_catalog.clone(),
                        (*schema).to_string(),
                    )
                }
                [catalog, schema] => ((*catalog).to_string(), (*schema).to_string()),
                _ => return Ok(None),
            };
            let Some(catalog) = ctx.catalog_registry().foreign_catalog(&catalog_name) else {
                return Ok(None);
            };
            if catalog.schema(&schema_name).is_some() {
                if cmd.if_not_exists {
                    return Ok(Some(ctx.ctx.read_empty()?));
                }
                return exec_err!("Schema '{schema_name}' already exists");
            }
            catalog.create(
                &schema_name,
                &CreateSchemaOptions {
                    mode: if cmd.if_not_exists {
                        CreateMode::CreateOrIgnore
                    } else {
                        CreateMode::Create
                    },
                },
            )?;
            Ok(Some(ctx.ctx.read_empty()?))
        }
        DdlStatement::CreateExternalTable(cmd) => {
            let state = ctx.ctx.state();
            let catalog_options = &state.config_options().catalog;
            let resolved = cmd.name.clone().resolve(
                &catalog_options.default_catalog,
                &catalog_options.default_schema,
            );
            let Some(catalog) = ctx.catalog_registry().foreign_catalog(&resolved.catalog) else {
                return Ok(None);
            };
            let Some(schema) = catalog.schema(&resolved.schema) else {
                return Ok(None);
            };

            let mode = match (cmd.if_not_exists, cmd.or_replace) {
                (true, true) => return exec_err!("'IF NOT EXISTS' cannot coexist with 'REPLACE'"),
                (true, false) => CreateMode::CreateOrIgnore,
                (false, true) => CreateMode::Replace,
                (false, false) => CreateMode::Create,
            };
            match (mode, schema.table_exist(&resolved.table)) {
                (CreateMode::CreateOrIgnore, true) => {
                    return Ok(Some(ctx.ctx.read_empty()?));
                }
                (CreateMode::Create, true) => {
                    return exec_err!("External table '{}' already exists", cmd.name)
                }
                _ => {}
            }

            let file_type = cmd.file_type.to_uppercase();
            let factory = state
                .table_factories()
                .get(file_type.as_str())
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Unable to find factory for {}",
                        cmd.file_type
                    ))
                })?;
            let provider = factory.create(&state, cmd).await?;
            let input = provider.scan(&state, None, &[], None).await?;
            let create = schema.create(
                &state,
                &resolved.table,
                &CreateTableOptions {
                    mode,
                    temporary: cmd.temporary,
                },
                input,
            )?;
            Ok(Some(ctx.ctx.read_table(Arc::new(CatalogDdlProvider {
                plan: create,
            }))?))
        }
        DdlStatement::CreateMemoryTable(cmd) => {
            if targets_foreign_catalog(ctx, &cmd.name) {
                return exec_err!(
                    "CREATE TABLE AS is not supported for foreign table '{}'; use CREATE EXTERNAL TABLE instead",
                    cmd.name
                );
            }
            Ok(None)
        }
        DdlStatement::CreateView(cmd) => {
            if targets_foreign_catalog(ctx, &cmd.name) {
                return exec_err!(
                    "Creating views is not supported for foreign view '{}'",
                    cmd.name
                );
            }
            Ok(None)
        }
        DdlStatement::DropTable(cmd) => {
            drop_foreign_object(ctx, &cmd.name, cmd.if_exists, CatalogObjectType::Table)
        }
        DdlStatement::DropView(cmd) => {
            drop_foreign_object(ctx, &cmd.name, cmd.if_exists, CatalogObjectType::View)
        }
        DdlStatement::DropCatalogSchema(cmd) => drop_foreign_schema(
            ctx,
            &cmd.name,
            cmd.if_exists,
            &DropSchemaOptions {
                cascade: cmd.cascade,
            },
        ),
        _ => Ok(None),
    }
}

fn targets_foreign_catalog(ctx: &SedonaContext, name: &TableReference) -> bool {
    let state = ctx.ctx.state();
    let catalog_options = &state.config_options().catalog;
    let resolved = name.clone().resolve(
        &catalog_options.default_catalog,
        &catalog_options.default_schema,
    );
    ctx.catalog_registry()
        .foreign_catalog(&resolved.catalog)
        .is_some()
}

fn drop_foreign_object(
    ctx: &SedonaContext,
    name: &TableReference,
    if_exists: bool,
    object_type: CatalogObjectType,
) -> Result<Option<DataFrame>, DataFusionError> {
    let state = ctx.ctx.state();
    let catalog_options = &state.config_options().catalog;
    let resolved = name.clone().resolve(
        &catalog_options.default_catalog,
        &catalog_options.default_schema,
    );
    let Some(catalog) = ctx.catalog_registry().foreign_catalog(&resolved.catalog) else {
        return Ok(None);
    };
    let drop_plan = match catalog.schema(&resolved.schema) {
        Some(schema) => schema.drop_table(
            &resolved.table,
            &DropTableOptions {
                object_type: Some(object_type),
                purge: false,
            },
        )?,
        None => None,
    };

    if let Some(plan) = drop_plan {
        return Ok(Some(
            ctx.ctx.read_table(Arc::new(CatalogDdlProvider { plan }))?,
        ));
    }
    if if_exists {
        return Ok(Some(ctx.ctx.read_empty()?));
    }

    let kind = match object_type {
        CatalogObjectType::Table => "Table",
        CatalogObjectType::View => "View",
    };
    exec_err!("{kind} '{name}' doesn't exist.")
}

fn drop_foreign_schema(
    ctx: &SedonaContext,
    name: &SchemaReference,
    if_exists: bool,
    options: &DropSchemaOptions,
) -> Result<Option<DataFrame>, DataFusionError> {
    let catalog_name = match name {
        SchemaReference::Full { catalog, .. } => catalog.as_ref(),
        SchemaReference::Bare { .. } => {
            let state = ctx.ctx.state();
            return drop_foreign_schema_in_catalog(
                ctx,
                &state.config_options().catalog.default_catalog,
                name,
                if_exists,
                options,
            );
        }
    };
    drop_foreign_schema_in_catalog(ctx, catalog_name, name, if_exists, options)
}

fn drop_foreign_schema_in_catalog(
    ctx: &SedonaContext,
    catalog_name: &str,
    name: &SchemaReference,
    if_exists: bool,
    options: &DropSchemaOptions,
) -> Result<Option<DataFrame>, DataFusionError> {
    let Some(catalog) = ctx.catalog_registry().foreign_catalog(catalog_name) else {
        return Ok(None);
    };
    if let Some(plan) = catalog.drop_schema(name.schema_name(), options)? {
        return Ok(Some(
            ctx.ctx.read_table(Arc::new(CatalogDdlProvider { plan }))?,
        ));
    }
    if if_exists {
        return Ok(Some(ctx.ctx.read_empty()?));
    }
    exec_err!("Schema '{name}' doesn't exist.")
}

/// Presents a catalog DDL execution plan as a one-shot DataFrame so DDL keeps
/// SedonaDB's lazy `.execute()`/`.collect()` behavior.
struct CatalogDdlProvider {
    plan: Arc<dyn ExecutionPlan>,
}

impl Debug for CatalogDdlProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogDdlProvider")
            .field("plan", &self.plan)
            .finish()
    }
}

#[async_trait]
impl TableProvider for CatalogDdlProvider {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.plan.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[datafusion_expr::Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self.plan.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    use arrow_schema::Schema;
    use datafusion::common::plan_datafusion_err;
    use datafusion::common::plan_err;
    use datafusion::datasource::listing::ListingTableUrl;
    use datafusion::sql::parser::DFParser;
    use datafusion_execution::TaskContext;
    use datafusion_expr::sqlparser::dialect::dialect_from_str;
    use datafusion_physical_plan::empty::EmptyExec;
    use datafusion_physical_plan::{
        DisplayAs, DisplayFormatType, PlanProperties, SendableRecordBatchStream,
    };
    use sedona_catalog::{DropSchemaOptions, SedonaCatalog, SedonaCatalogList, SedonaSchema};
    use url::Url;

    use super::*;

    struct DropTestExec {
        inner: Arc<dyn ExecutionPlan>,
        drop_action: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }

    impl DropTestExec {
        fn new(drop_action: impl FnOnce() + Send + 'static) -> Self {
            Self {
                inner: Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
                drop_action: Mutex::new(Some(Box::new(drop_action))),
            }
        }
    }

    impl Debug for DropTestExec {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("DropTestExec").finish()
        }
    }

    impl DisplayAs for DropTestExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "DropTestExec")
        }
    }

    impl ExecutionPlan for DropTestExec {
        fn name(&self) -> &str {
            "DropTestExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.inner.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            if !children.is_empty() {
                return datafusion_common::internal_err!("DropTestExec does not have children");
            }
            Ok(self)
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            if let Some(drop_action) = self.drop_action.lock().unwrap().take() {
                drop_action();
            }
            self.inner.execute(partition, context)
        }
    }

    #[derive(Debug)]
    struct DropTestCatalogList {
        catalog: Arc<dyn SedonaCatalog>,
    }

    impl SedonaCatalogList for DropTestCatalogList {
        fn catalog_names(&self) -> Vec<String> {
            vec!["foreign".to_owned()]
        }

        fn catalog(&self, name: &str) -> Option<Arc<dyn SedonaCatalog>> {
            (name == "foreign").then(|| self.catalog.clone())
        }

        fn create(
            &self,
            _name: &str,
            _options: &CreateCatalogOptions,
        ) -> Result<Arc<dyn SedonaCatalog>> {
            datafusion_common::not_impl_err!("not needed by drop tests")
        }
    }

    #[derive(Debug)]
    struct DropTestCatalog {
        schema: Arc<dyn SedonaSchema>,
        schema_exists: Arc<AtomicBool>,
    }

    impl SedonaCatalog for DropTestCatalog {
        fn schema_names(&self) -> Vec<String> {
            self.schema_exists
                .load(Ordering::SeqCst)
                .then(|| vec!["public".to_owned()])
                .unwrap_or_default()
        }

        fn schema(&self, name: &str) -> Option<Arc<dyn SedonaSchema>> {
            (name == "public" && self.schema_exists.load(Ordering::SeqCst))
                .then(|| self.schema.clone())
        }

        fn create(
            &self,
            _name: &str,
            _options: &CreateSchemaOptions,
        ) -> Result<Arc<dyn SedonaSchema>> {
            datafusion_common::not_impl_err!("not needed by drop tests")
        }

        fn drop_schema(
            &self,
            name: &str,
            options: &DropSchemaOptions,
        ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
            if name != "public" || !self.schema_exists.load(Ordering::SeqCst) {
                return Ok(None);
            }
            if !options.cascade {
                return datafusion_common::exec_err!("public must be dropped with cascade");
            }
            let schema_exists = self.schema_exists.clone();
            Ok(Some(Arc::new(DropTestExec::new(move || {
                schema_exists.store(false, Ordering::SeqCst);
            }))))
        }
    }

    #[derive(Debug)]
    struct DropTestSchema {
        objects: Arc<Mutex<HashMap<String, CatalogObjectType>>>,
        received: Mutex<Vec<DropTableOptions>>,
    }

    #[async_trait]
    impl SedonaSchema for DropTestSchema {
        fn table_names(&self) -> Vec<String> {
            self.objects.lock().unwrap().keys().cloned().collect()
        }

        async fn table(&self, _name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
            Ok(None)
        }

        fn create(
            &self,
            _session: &dyn Session,
            _name: &str,
            _options: &CreateTableOptions,
            _input: Arc<dyn ExecutionPlan>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            datafusion_common::not_impl_err!("not needed by drop tests")
        }

        fn drop_table(
            &self,
            name: &str,
            options: &DropTableOptions,
        ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
            self.received.lock().unwrap().push(*options);
            let objects = self.objects.lock().unwrap();
            let Some(actual_type) = objects.get(name).copied() else {
                return Ok(None);
            };
            if options.object_type.is_some() && options.object_type != Some(actual_type) {
                return Ok(None);
            }
            drop(objects);
            let objects = self.objects.clone();
            let name = name.to_owned();
            Ok(Some(Arc::new(DropTestExec::new(move || {
                objects.lock().unwrap().remove(&name);
            }))))
        }

        fn table_exist(&self, name: &str) -> bool {
            self.objects.lock().unwrap().contains_key(name)
        }
    }

    #[tokio::test]
    async fn foreign_drop_distinguishes_tables_and_views() -> Result<()> {
        let schema = Arc::new(DropTestSchema {
            objects: Arc::new(Mutex::new(HashMap::from([
                ("table_one".to_owned(), CatalogObjectType::Table),
                ("view_one".to_owned(), CatalogObjectType::View),
            ]))),
            received: Mutex::new(Vec::new()),
        });
        let catalog: Arc<dyn SedonaCatalog> = Arc::new(DropTestCatalog {
            schema: schema.clone(),
            schema_exists: Arc::new(AtomicBool::new(true)),
        });
        let ctx = SedonaContext::new();
        ctx.register_catalog_list(Arc::new(DropTestCatalogList { catalog }));

        let drop_table = ctx.sql("DROP TABLE foreign.public.table_one").await?;
        assert!(schema.table_exist("table_one"));
        drop_table.collect().await?;
        assert!(!schema.table_exist("table_one"));

        let error = ctx
            .sql("DROP TABLE foreign.public.view_one")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("doesn't exist"));
        assert!(schema.table_exist("view_one"));

        let drop_view = ctx.sql("DROP VIEW foreign.public.view_one").await?;
        assert!(schema.table_exist("view_one"));
        drop_view.collect().await?;
        assert!(!schema.table_exist("view_one"));
        assert_eq!(
            *schema.received.lock().unwrap(),
            vec![
                DropTableOptions {
                    object_type: Some(CatalogObjectType::Table),
                    purge: false,
                },
                DropTableOptions {
                    object_type: Some(CatalogObjectType::Table),
                    purge: false,
                },
                DropTableOptions {
                    object_type: Some(CatalogObjectType::View),
                    purge: false,
                },
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn foreign_drop_schema_is_lazy() -> Result<()> {
        let schema: Arc<dyn SedonaSchema> = Arc::new(DropTestSchema {
            objects: Arc::new(Mutex::new(HashMap::new())),
            received: Mutex::new(Vec::new()),
        });
        let catalog = Arc::new(DropTestCatalog {
            schema,
            schema_exists: Arc::new(AtomicBool::new(true)),
        });
        let ctx = SedonaContext::new();
        ctx.register_catalog_list(Arc::new(DropTestCatalogList {
            catalog: catalog.clone(),
        }));

        let drop_schema = ctx.sql("DROP SCHEMA foreign.public CASCADE").await?;
        assert!(catalog.schema("public").is_some());
        drop_schema.collect().await?;
        assert!(catalog.schema("public").is_none());

        Ok(())
    }

    #[tokio::test]
    async fn unsupported_foreign_create_does_not_drop_existing_objects() -> Result<()> {
        let schema = Arc::new(DropTestSchema {
            objects: Arc::new(Mutex::new(HashMap::from([
                ("table_one".to_owned(), CatalogObjectType::Table),
                ("view_one".to_owned(), CatalogObjectType::View),
            ]))),
            received: Mutex::new(Vec::new()),
        });
        let catalog: Arc<dyn SedonaCatalog> = Arc::new(DropTestCatalog {
            schema: schema.clone(),
            schema_exists: Arc::new(AtomicBool::new(true)),
        });
        let ctx = SedonaContext::new();
        ctx.register_catalog_list(Arc::new(DropTestCatalogList { catalog }));

        let table_error = ctx
            .sql("CREATE OR REPLACE TABLE foreign.public.table_one AS SELECT 1")
            .await
            .unwrap_err();
        assert!(table_error
            .to_string()
            .contains("use CREATE EXTERNAL TABLE instead"));
        assert!(schema.table_exist("table_one"));

        let view_error = ctx
            .sql("CREATE OR REPLACE VIEW foreign.public.view_one AS SELECT 1")
            .await
            .unwrap_err();
        assert!(view_error
            .to_string()
            .contains("Creating views is not supported"));
        assert!(schema.table_exist("view_one"));
        assert!(schema.received.lock().unwrap().is_empty());

        Ok(())
    }

    async fn create_external_table_test(location: &str, sql: &str) -> Result<()> {
        let ctx = SedonaContext::new();
        let plan = ctx.ctx.state().create_logical_plan(sql).await?;

        if let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) = &plan {
            register_object_store_and_config_extensions(&ctx, &cmd.location, &cmd.options).await?;
        } else {
            return plan_err!("LogicalPlan is not a CreateExternalTable");
        }

        // Ensure the URL is supported by the object store
        ctx.ctx
            .runtime_env()
            .object_store(ListingTableUrl::parse(location)?)?;

        Ok(())
    }

    async fn copy_to_table_test(location: &str, sql: &str) -> Result<()> {
        let ctx = SedonaContext::new();
        // AWS CONFIG register.

        let plan = ctx.ctx.state().create_logical_plan(sql).await?;

        if let LogicalPlan::Copy(cmd) = &plan {
            register_object_store_and_config_extensions(&ctx, &cmd.output_url, &cmd.options)
                .await?;
        } else {
            return plan_err!("LogicalPlan is not a CreateExternalTable");
        }

        // Ensure the URL is supported by the object store
        ctx.ctx
            .runtime_env()
            .object_store(ListingTableUrl::parse(location)?)?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_http() -> Result<()> {
        // Should be OK
        let location = "http://example.com/file.parquet";
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }
    #[tokio::test]
    async fn copy_to_external_object_store_test() -> Result<()> {
        let locations = vec![
            "s3://bucket/path/file.parquet",
            "oss://bucket/path/file.parquet",
            "cos://bucket/path/file.parquet",
            "gcs://bucket/path/file.parquet",
        ];
        let ctx = SedonaContext::new();
        let task_ctx = ctx.ctx.task_ctx();
        let dialect = &task_ctx.session_config().options().sql_parser.dialect;
        let dialect = dialect_from_str(dialect).ok_or_else(|| {
            plan_datafusion_err!(
                "Unsupported SQL dialect: {dialect}. Available dialects: \
                 Generic, MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, \
                 MsSQL, ClickHouse, BigQuery, Ansi, DuckDB, Databricks."
            )
        })?;
        for location in locations {
            let sql = format!("copy (values (1,2)) to '{location}' STORED AS PARQUET;");
            let statements = DFParser::parse_sql_with_dialect(&sql, dialect.as_ref())?;
            for statement in statements {
                //Should not fail
                let mut plan = create_plan_from_sql(&ctx, statement).await?;
                if let LogicalPlan::Copy(copy_to) = &mut plan {
                    assert_eq!(copy_to.output_url, location);
                    assert_eq!(copy_to.file_type.get_ext(), "parquet".to_string());
                    ctx.ctx
                        .runtime_env()
                        .object_store_registry
                        .get_store(&Url::parse(&copy_to.output_url).unwrap())?;
                } else {
                    return plan_err!("LogicalPlan is not a CopyTo");
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn copy_to_object_store_table_s3() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let location = "s3://bucket/path/file.parquet";

        // Missing region, use object_store defaults
        let sql = format!("COPY (values (1,2)) TO '{location}' STORED AS PARQUET
            OPTIONS ('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}')");
        copy_to_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_s3() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let region = "fake_us-east-2";
        let session_token = "fake_session_token";
        let location = "s3://bucket/path/file.parquet";

        // Missing region, use object_store defaults
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        // Should be OK
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}', 'aws.region' '{region}', 'aws.session_token' '{session_token}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_oss() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let endpoint = "fake_endpoint";
        let location = "oss://bucket/path/file.parquet";

        // Should be OK
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}', 'aws.oss.endpoint' '{endpoint}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_cos() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let endpoint = "fake_endpoint";
        let location = "cos://bucket/path/file.parquet";

        // Should be OK
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}', 'aws.cos.endpoint' '{endpoint}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_gcs() -> Result<()> {
        let service_account_path = "fake_service_account_path";
        let service_account_key =
            "{\"private_key\": \"fake_private_key.pem\",\"client_email\":\"fake_client_email\", \"private_key_id\":\"id\"}";
        let application_credentials_path = "fake_application_credentials_path";
        let location = "gcs://bucket/path/file.parquet";

        // for service_account_path
        let sql = format!(
            "CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('gcp.service_account_path' '{service_account_path}') LOCATION '{location}'"
        );
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("os error 2"));

        // for service_account_key
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET OPTIONS('gcp.service_account_key' '{service_account_key}') LOCATION '{location}'");
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("no items found"), "{err}");

        // for application_credentials_path
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('gcp.application_credentials_path' '{application_credentials_path}') LOCATION '{location}'");
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("os error 2"));

        Ok(())
    }

    #[tokio::test]
    async fn create_external_table_local_file() -> Result<()> {
        let location = "path/to/file.parquet";

        // Ensure that local files are also registered
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET LOCATION '{location}'");
        create_external_table_test(location, &sql).await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn create_external_table_format_option() -> Result<()> {
        let location = "path/to/file.cvs";

        // Test with format options
        let sql =
            format!("CREATE EXTERNAL TABLE test STORED AS CSV LOCATION '{location}' OPTIONS('format.has_header' 'true')");
        create_external_table_test(location, &sql).await.unwrap();

        Ok(())
    }
}
