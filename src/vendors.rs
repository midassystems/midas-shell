pub mod v_databento;

use crate::error::{Error, Result};
use crate::tickers::Ticker;
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

pub trait Vendor {
    /// Update all active tickers for vendor to present data.
    async fn update<T: AsRef<Historical>>(&mut self, tickers: Vec<Ticker>, client: T)
        -> Result<()>;

    /// Download raw data to file from vendor.
    async fn download(
        &mut self,
        tickers: &Vec<String>,
        schema: Schema,
        start: OffsetDateTime,
        end: OffsetDateTime,
        dataset: &str,
        stype: &str,
    ) -> Result<(DownloadType, PathBuf)>;

    /// Transforms data from vendor format to mbn format.
    async fn transform(
        &self,
        mbn_map: &HashMap<String, u32>,
        dbn_path: &PathBuf,
        mbn_filename: &PathBuf,
    ) -> Result<PathBuf>;

    /// Loads to database, deletes MBN file after
    async fn load<T: AsRef<Historical>>(
        &self,
        mbn_map: HashMap<String, u32>,
        download_type: &DownloadType,
        download_path: &PathBuf,
        mbn_filename: &PathBuf,
        client: &T,
    ) -> Result<()>;
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::error::Result;
//     use crate::tickers::get_tickers;
//     use crate::utils::load_file;
//     use crate::vendors::v_databento::{
//         extract::read_dbn_file,
//         transform::{instrument_id_map, to_mbn},
//         utils::databento_file_path,
//     };
//     use databento::dbn::{Dataset, Schema};
//     use serial_test::serial;
//     use std::path::PathBuf;
//     use std::sync::Arc;
//     use time;
//
//     fn setup(dir_path: &PathBuf) -> Result<PathBuf> {
//         // Parameters
//         let dataset = Dataset::GlbxMdp3;
//         let start = time::macros::datetime!(2024-08-20 00:00 UTC);
//         let end = time::macros::datetime!(2024-08-20 05:00 UTC);
//         let schema = Schema::Mbp1;
//         let symbols = vec!["ZM.n.0".to_string(), "GC.n.0".to_string()];
//
//         // Construct file path
//         let file_path = databento_file_path(dir_path, &dataset, &schema, &start, &end, &symbols)?;
//
//         Ok(file_path)
//     }
//
//     #[tokio::test]
//     #[serial]
//     // #[ignore]
//     async fn test_load_file_to_db() -> Result<()> {
//         let base_url = "http://localhost:8080"; // Update with your actual base URL
//         let client = Arc::new(Historical::new(base_url));
//
//         // Create Instruments
//         let tickers = get_tickers("tests/tickers.json", "databento", &client).await?;
//         // println!("{:?}", mbn_map);
//         let mut mbn_map = HashMap::new();
//         for ticker in tickers {
//             mbn_map.insert(ticker.ticker.clone(), ticker.get_mbn_id()?);
//         }
//
//         // Load DBN file
//         let file_path = setup(&PathBuf::from("tests/data/databento"))?;
//         // let file_path = setup("tests/data/databento").unwrap();
//         let (mut records, dbn_map) = read_dbn_file(file_path).await?;
//
//         // Create the new map
//         let new_map = instrument_id_map(dbn_map, mbn_map.clone())?;
//         let mbn_file_name = PathBuf::from("../data/testing_file.bin");
//         let _ = to_mbn(&mut records, &new_map, &mbn_file_name).await?;
//         let _ = drop(records);
//
//         // Test
//         let path = PathBuf::from("data/testing_file.bin");
//         let _ = load_file(&path, &client).await?;
//
//         // Cleanup
//         if mbn_file_name.exists() {
//             std::fs::remove_file(&mbn_file_name).expect("Failed to delete the test file.");
//         }
//
//         for value in mbn_map.values() {
//             let _ = client.delete_symbol(&(*value as i32)).await?;
//         }
//
//         Ok(())
//     }
// }
