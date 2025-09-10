use datafusion::config::TableParquetOptions;

pub struct TableGeoParquetOptions {
    pub inner: TableParquetOptions,
}
