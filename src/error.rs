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
    #[error("Mbinary error: {0}")]
    MbinaryError(#[from] mbinary::error::Error),
    #[error("Request error: {0}")]
    TracingError(#[from] tracing::subscriber::SetGlobalDefaultError),
    // #[error("Anyhow error: {0}")]
    // AnyhowError(#[from] anyhow::Error),
    #[error("Invalid date format: {0}")]
    InvalidDateFormat(String),
}

#[macro_export]
macro_rules! error {
    ($variant:ident, $($arg:tt)*) => {
        Error::$variant(format!($($arg)*))
    };
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_macro() {
        let error = error!(CustomError, "Testing 123 : {}", 69);
        let x_error = Error::CustomError(format!("Testing 123 : {}", 69));

        // Test
        assert_eq!(error.to_string(), x_error.to_string());
    }
}
