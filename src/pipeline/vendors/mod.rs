pub mod v_databento;

use crate::error::{Error, Result};
use async_trait::async_trait;
use databento::dbn::Schema;
use midas_client::historical::Historical;
use std::path::PathBuf;
use time::{self, OffsetDateTime};

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
    async fn update(&mut self, client: &Historical) -> Result<()>;

    /// Download raw data to file from vendor.
    async fn download(
        &mut self,
        tickers: &Vec<String>,
        schema: Schema,
        start: OffsetDateTime,
        end: OffsetDateTime,
        dataset: &str,
        stype: &str,
        dir_path: Option<String>,
    ) -> Result<(DownloadType, PathBuf)>;

    /// Transforms data from vendor format to mbn format, saves to staging file.
    async fn transform(
        &self,
        download_path: &PathBuf,
        mbn_filename: &PathBuf,
        client: &Historical,
        env_dirs: bool,
    ) -> Result<PathBuf>;

    /// Transforms data from vendor format to mbn format, saves to staging file.
    async fn stage(
        &self,
        download_type: &DownloadType,
        dbn_path: &PathBuf,
        mbn_filename: &PathBuf,
        client: &Historical,
    ) -> Result<Vec<PathBuf>>;

    /// Loads to database, deletes staging file after.
    async fn upload(&self, client: &Historical, files: Vec<PathBuf>) -> Result<()>;
}
