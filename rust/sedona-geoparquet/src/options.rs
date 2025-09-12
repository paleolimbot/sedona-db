use datafusion::config::TableParquetOptions;

/// [TableParquetOptions] wrapper with GeoParquet-specific options
#[derive(Debug, Default, Clone)]
pub struct TableGeoParquetOptions {
    pub inner: TableParquetOptions,
    pub geoparquet_version: GeoParquetVersion,
}

impl From<TableParquetOptions> for TableGeoParquetOptions {
    fn from(value: TableParquetOptions) -> Self {
        Self {
            inner: value,
            geoparquet_version: GeoParquetVersion::default(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum GeoParquetVersion {
    Auto,
    V1_0,
    V1_1,
    V2_0,
    Omitted,
}

impl Default for GeoParquetVersion {
    fn default() -> Self {
        Self::Auto
    }
}
