pub mod v_databento;

use crate::error::{Error, Result};
use async_trait::async_trait;
use mbinary::enums::Dataset;
use midas_client::historical::Historical;
use midas_client::instrument::Instruments;
use std::path::PathBuf;

#[derive(Debug, PartialEq, Eq)]
pub enum DownloadType {
    Stream,
    Batch,
}

impl TryFrom<&str> for DownloadType {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        match value.to_uppercase().as_str() {
            "STREAM" => Ok(Self::Stream),
            "BATCH" => Ok(Self::Batch),
            _ => Err(Error::InvalidDownloadType),
        }
    }
}
#[async_trait]
pub trait Vendor {
    /// Update all active tickers for vendor to present data.
    async fn update(
        &mut self,
        dataset: Dataset,
        hist_client: &Historical,
        instrument_client: &Instruments,
        download_approval: bool,
    ) -> Result<()>;

    /// Transforms data from vendor format to mbinary format, saves to staging file.
    async fn transform(
        &self,
        dataset: Dataset,
        download_path: &PathBuf,
        mbinary_filename: &PathBuf,
        instrument_client: &Instruments,
        env_dirs: bool,
    ) -> Result<PathBuf>;

    /// Transforms data from vendor format to mbinary format, saves to staging file.
    async fn stage(
        &self,
        dataset: Dataset,
        download_type: &DownloadType,
        dbn_path: &PathBuf,
        mbinary_filename: &PathBuf,
        instrument_client: &Instruments,
    ) -> Result<Vec<PathBuf>>;

    /// Loads to database, deletes staging file after.
    async fn upload(&self, client: &Historical, files: Vec<PathBuf>) -> Result<()>;
}
