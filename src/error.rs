use std::env::VarError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Io error: {0}")]
    EnvVarError(#[from] VarError),
    #[error("General error: {0}")]
    GeneralError(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Api Error: {0}")]
    ApiError(String),
    #[error("Reedline Error: {0}")]
    Reedline(#[from] reedline::ReedlineError),
    #[error("Midas Api Error: {0}")]
    MidasApiError(#[from] midas_client::error::Error),
    #[error("Custom error: {0}")]
    CustomError(String),
    #[error("Date error: {0}")]
    DateError(String),
    #[error("Ticker Loading error: {0}")]
    TickerLoading(String),
    #[error("Serde Json error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("Conversion error: {0}")]
    Conversion(String),
    #[error("Databent error: {0}")]
    DatabentoError(#[from] databento::Error),
    #[error("Dbn error: {0}")]
    DbnError(#[from] databento::dbn::Error),
    #[error("No data was returned")]
    NoDataError,
    #[error("File not found: {0}")]
    FileNotFoundError(String),
    #[error("Invalid DatabentoDownloadType")]
    InvalidDownloadType,
    #[error("Time Format error: {0}")]
    FormatError(#[from] time::error::Format),
    #[error("MBN error: {0}")]
    MbnError(#[from] mbn::error::Error),
    // #[error("Vendor error: {0}")]
    // VendorError(#[from] vendors::error::Error),
    // #[error("Data Error: {0}")]
    // DataError(#[from] data_sources::error::Error),
    // #[error("Parsing Error: {0}")]
    // ParsingError(#[from] time::error::Parse),
    // #[error("Config Error: {0}")]
    // ConfigError(#[from] config::error::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
