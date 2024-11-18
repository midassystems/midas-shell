pub mod v_databento;

use crate::error::{Error, Result};
use crate::tickers::Ticker;
use async_trait::async_trait;
use databento::dbn::Schema;
use midas_client::historical::Historical;
use std::collections::HashMap;
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
    async fn update(&mut self, tickers: Vec<Ticker>, client: &Historical) -> Result<()>;

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

    /// Transforms data from vendor format to mbn format.
    async fn transform(
        &self,
        mbn_map: &HashMap<String, u32>,
        dbn_path: &PathBuf,
        mbn_filename: &PathBuf,
    ) -> Result<PathBuf>;

    /// Loads to database, deletes MBN file after
    async fn load(
        &self,
        mbn_map: HashMap<String, u32>,
        download_type: &DownloadType,
        download_path: &PathBuf,
        mbn_filename: &PathBuf,
        client: &Historical,
    ) -> Result<()>;
}
