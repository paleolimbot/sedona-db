use arrow_schema::ArrowError;
use datafusion_common::DataFusionError;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum RSedonaError {
    #[error("{0}")]
    DF(Box<DataFusionError>),
    #[error("{0}")]
    TokioIO(tokio::io::Error),
    #[error("{0}")]
    TokioJoin(tokio::task::JoinError),
    #[error("{0}")]
    Internal(String),
    #[error("Interrupted")]
    Interrupted,
}

impl From<DataFusionError> for RSedonaError {
    fn from(other: DataFusionError) -> Self {
        RSedonaError::DF(Box::new(other))
    }
}

impl From<tokio::io::Error> for RSedonaError {
    fn from(other: tokio::io::Error) -> Self {
        RSedonaError::TokioIO(other)
    }
}

impl From<tokio::task::JoinError> for RSedonaError {
    fn from(other: tokio::task::JoinError) -> Self {
        RSedonaError::TokioJoin(other)
    }
}

impl From<ArrowError> for RSedonaError {
    fn from(other: ArrowError) -> Self {
        RSedonaError::DF(Box::new(DataFusionError::ArrowError(Box::new(other), None)))
    }
}
