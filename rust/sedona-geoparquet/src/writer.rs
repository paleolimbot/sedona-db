use datafusion::config::TableParquetOptions;

#[derive(Debug, Default)]
pub struct TableGeoParquetOptions {
    pub inner: TableParquetOptions,
}
