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
use datafusion::catalog::{
    CatalogProvider, CatalogProviderList, SchemaProvider, Session, TableProvider,
};
use datafusion::prelude::DataFrame;
use datafusion_common::exec_err;
use datafusion_expr::{DdlStatement, LogicalPlan, TableType};
use datafusion_physical_plan::ExecutionPlan;
use sedona_catalog::{DataFusionCatalog, DataFusionSchema, OverlayCatalogList};
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
            let state = ctx.ctx.state();
            let Some(catalogs) = state.catalog_list().downcast_ref::<OverlayCatalogList>() else {
                return Ok(None);
            };
            if catalogs.catalog(&cmd.catalog_name).is_some() {
                if cmd.if_not_exists {
                    return Ok(Some(ctx.ctx.read_empty()?));
                }
                return exec_err!("Catalog '{}' already exists", cmd.catalog_name);
            }
            catalogs.foreign().create(&cmd.catalog_name)?;
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
            let Some(catalog) = ctx.ctx.catalog(&catalog_name) else {
                return Ok(None);
            };
            let Some(catalog) = catalog.downcast_ref::<DataFusionCatalog>() else {
                return Ok(None);
            };
            if catalog.schema(&schema_name).is_some() {
                if cmd.if_not_exists {
                    return Ok(Some(ctx.ctx.read_empty()?));
                }
                return exec_err!("Schema '{schema_name}' already exists");
            }
            catalog.inner().create(&schema_name)?;
            Ok(Some(ctx.ctx.read_empty()?))
        }
        DdlStatement::CreateExternalTable(cmd) => {
            if cmd.temporary {
                return Ok(None);
            }
            let state = ctx.ctx.state();
            let catalog_options = &state.config_options().catalog;
            let resolved = cmd.name.clone().resolve(
                &catalog_options.default_catalog,
                &catalog_options.default_schema,
            );
            let Some(catalog) = state.catalog_list().catalog(&resolved.catalog) else {
                return Ok(None);
            };
            let Some(schema) = catalog.schema(&resolved.schema) else {
                return Ok(None);
            };
            let Some(schema) = schema.downcast_ref::<DataFusionSchema>() else {
                return Ok(None);
            };

            let exists = schema.table_exist(&resolved.table);
            match (cmd.if_not_exists, cmd.or_replace, exists) {
                (true, false, true) => return Ok(Some(ctx.ctx.read_empty()?)),
                (true, true, true) => {
                    return exec_err!("'IF NOT EXISTS' cannot coexist with 'REPLACE'")
                }
                (false, false, true) => {
                    return exec_err!("External table '{}' already exists", cmd.name)
                }
                (false, true, true) => {
                    schema.inner().deregister(&resolved.table)?;
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
            let create = schema.inner().create(&resolved.table, input)?;
            Ok(Some(ctx.ctx.read_table(Arc::new(CatalogDdlProvider {
                plan: create,
            }))?))
        }
        _ => Ok(None),
    }
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

    use datafusion::common::plan_datafusion_err;
    use datafusion::common::plan_err;
    use datafusion::datasource::listing::ListingTableUrl;
    use datafusion::sql::parser::DFParser;
    use datafusion_expr::sqlparser::dialect::dialect_from_str;
    use url::Url;

    use super::*;

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
