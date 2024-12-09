pub mod client;
pub mod compare;
pub mod extract;
pub mod transform;
pub mod utils;

use super::super::utils::get_earlier_of_year_end_or_date;
use crate::error;
use crate::pipeline::midas::checks::find_duplicates;
//Macro
use crate::pipeline::vendors::{DownloadType, Vendor};
use crate::{Error, Result};
use async_trait::async_trait;
use client::DatabentoClient;
use databento::dbn::{Dataset, SType, Schema};
use extract::{read_dbn_batch_dir, read_dbn_file};
use mbn::symbols::Instrument;
use midas_client::historical::Historical;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use time::{macros::time, OffsetDateTime};
use transform::{instrument_id_map, to_mbn};

pub struct DatabentoVendor {
    databento_client: DatabentoClient,
}

impl DatabentoVendor {
    /// Create a new DatabentoVendor instance.
    pub fn new(api_key: &String) -> Result<Self> {
        let databento_client = DatabentoClient::new(api_key)?;
        Ok(Self { databento_client })
    }

    async fn update_ticker(
        &mut self,
        ticker: &Instrument,
        start: &OffsetDateTime,
        end: &OffsetDateTime,
        stype: &str,
        dataset: &str,
        client: &Historical,
    ) -> Result<()> {
        // Download
        let (download_type, file_name) = self
            .download(
                &vec![ticker.ticker.clone()],
                Schema::Mbp1,
                *start,
                *end,
                &dataset,
                &stype,
                None,
            )
            .await?;

        // Mbn file path
        let mbn_filename = PathBuf::from(format!(
            "{}_{}_{}_{}.bin",
            &ticker.ticker,
            &stype,
            start.date(),
            end.date()
        ));

        // Stage
        let files = self
            .stage(
                // mbn_map,
                &download_type,
                &file_name,
                &mbn_filename,
                client,
            )
            .await?;

        // Upload
        let _ = self.upload(client, files).await?;

        // Update instrument
        Ok(())
    }
}

#[async_trait]
impl Vendor for DatabentoVendor {
    async fn update(&mut self, client: &Historical) -> Result<()> {
        // Calculate today at the start of the day once
        let today = OffsetDateTime::now_utc().replace_time(time!(00:00));

        // Get tickers
        let api_response = client.list_vendor_symbols(&"databento".to_string()).await?;
        let tickers: Vec<Instrument> = api_response.data;

        // Iterate over different request
        for mut ticker in tickers {
            let mut end_flag = false;
            let instrument_id = ticker.instrument_id.ok_or_else(|| {
                error!(
                    CustomError,
                    "Ticker {} has no instrument_id",
                    ticker.ticker.clone()
                )
            })?;

            let stype = ticker.stype.as_ref().ok_or_else(|| {
                error!(
                    CustomError,
                    "Ticker {} has no stype.",
                    ticker.ticker.clone()
                )
            })?;

            let dataset = ticker.dataset.as_ref().ok_or_else(|| {
                error!(
                    CustomError,
                    "Ticker {} has no dataset.",
                    ticker.ticker.clone()
                )
            })?;

            while !end_flag {
                let start = ticker.last_available_datetime()?;
                let end = get_earlier_of_year_end_or_date(start, today);
                println!("Ticker {:?} Start {:?} End {:?}", ticker.ticker, start, end);

                if start == end {
                    println!("Ticker {:?} is already up-to-date.", ticker.ticker);
                    break; // Move to the next ticker
                }

                // Load data
                self.update_ticker(&ticker, &start, &end, &stype, &dataset, client)
                    .await
                    .map_err(|e| {
                        error!(
                            CustomError,
                            "Failed to upload ticker {} for start {} and end {} : {:?}",
                            ticker.ticker,
                            start,
                            end,
                            e
                        )
                    })?;

                // Update ticker last_available field
                ticker.last_available = end.unix_timestamp_nanos() as u64;
                client
                    .update_symbol(&ticker, &(instrument_id as i32))
                    .await
                    .map_err(|e| {
                        error!(
                            CustomError,
                            "Failed to update ticker {} last_available date {} : {:?}",
                            ticker.ticker,
                            ticker.last_available,
                            e
                        )
                    })?;

                // If the end date is today, we're done with this ticker
                if end == today {
                    end_flag = true;
                }
            }
        }

        Ok(())
    }

    async fn download(
        &mut self,
        tickers: &Vec<String>,
        schema: Schema,
        start: OffsetDateTime,
        end: OffsetDateTime,
        dataset: &str,
        stype: &str,
        dir_path: Option<String>,
    ) -> Result<(DownloadType, PathBuf)> {
        let dir;
        if let Some(path) = dir_path {
            dir = PathBuf::from(path);
        } else {
            let raw_dir = std::env::var("RAW_DIR")
                .map_err(|_| error!(CustomError, "Environment variable RAW_DIR is not set."))?;
            dir = PathBuf::from(raw_dir);
        }
        let (download_type, file_name) = self
            .databento_client
            .get_historical(
                &Dataset::from_str(&dataset)?,
                &start,
                &end,
                &tickers,
                &schema,
                &SType::from_str(&stype)?,
                &dir,
            )
            .await?
            .ok_or(Error::NoDataError)?;

        Ok((download_type, file_name))
    }

    async fn transform(
        &self,
        dbn_filename: &PathBuf,
        mbn_filename: &PathBuf,
        client: &Historical,
        env_dirs: bool,
    ) -> Result<PathBuf> {
        // -- Extract
        let dbn_filepath = if env_dirs {
            let raw_dir = env::var("RAW_DIR").expect("RAW_DIR not set.");
            &PathBuf::from(raw_dir).join("databento").join(dbn_filename)
        } else {
            dbn_filename
        };

        let mut records;
        let dbn_map;
        (records, dbn_map) = read_dbn_file(dbn_filepath.clone()).await?;

        // Mbn map
        let api_response = client.list_vendor_symbols(&"databento".to_string()).await?;
        let tickers: Vec<Instrument> = api_response.data;
        let mut mbn_map = HashMap::new();
        for ticker in tickers {
            let instrument_id = ticker.instrument_id.ok_or_else(|| {
                error!(
                    CustomError,
                    "Ticker {} has no instrument_id.",
                    ticker.ticker.clone()
                )
            })?;

            mbn_map.insert(ticker.ticker.clone(), instrument_id);
        }

        // -- TRANSFORM
        // Map DBN instrument to MBN insturment
        let mbn_filepath = if env_dirs {
            let processed_dir = env::var("PROCESSED_DIR").expect("PROCESSED_DIR not set.");
            &PathBuf::from(processed_dir).join(mbn_filename)
        } else {
            mbn_filename
        };

        let new_map = instrument_id_map(dbn_map, mbn_map.clone())?;
        let _ = to_mbn(&mut records, &new_map, mbn_filepath).await?;
        let _ = drop(records);

        // Check for duplicates
        let duplicates_count = find_duplicates(mbn_filepath).await?;

        if duplicates_count > 0 {
            std::fs::remove_file(mbn_filepath.clone())?;
        }

        println!("Staged data path : {:?}", mbn_filepath);

        Ok(mbn_filepath.clone())
    }

    async fn stage(
        &self,
        // mbn_map: HashMap<String, u32>,
        download_type: &DownloadType,
        download_path: &PathBuf,
        mbn_filename: &PathBuf,
        client: &Historical,
    ) -> Result<Vec<PathBuf>> {
        let mut files_list = Vec::new();

        if download_type == &DownloadType::Stream {
            let _ = self
                .transform(download_path, mbn_filename, client, true)
                .await?;
            files_list.push(mbn_filename.clone());
        } else {
            let raw_dir = env::var("RAW_DIR").expect("RAW_DIR not set.");
            let path = PathBuf::from(&raw_dir)
                .join("databento")
                .join(download_path);
            let files = read_dbn_batch_dir(&path).await?;

            let mut count = 0;
            let processed_dir = env::var("PROCESSED_DIR").expect("PROCESSED_DIR not set.");

            for file in files {
                let mbn_file = PathBuf::from(format!(
                    "{}_{}",
                    count,
                    mbn_filename.file_name().unwrap().to_string_lossy()
                ));

                let mbn_path = PathBuf::from(&processed_dir).join(&mbn_file);
                let _ = self.transform(&file, &mbn_path, client, false).await?;
                files_list.push(mbn_file);
                count += 1;
            }
        }

        Ok(files_list)
    }

    async fn upload(&self, client: &Historical, files: Vec<PathBuf>) -> Result<()> {
        let raw_dir = std::env::var("PROCESSED_DIR").map_err(|_| {
            error!(
                CustomError,
                "Environment variable PROCESSED_DIR is not set."
            )
        })?;
        let mut errors = Vec::new(); // To collect errors

        for file in &files {
            let file_string: String = file.to_string_lossy().into_owned();

            // Attempt to upload the file
            match client.create_mbp_from_file(&file_string).await {
                Ok(response) => {
                    println!("{:?}", response);
                }
                Err(e) => {
                    eprintln!("Error uploading file {}: {}", file.display(), e);
                    errors.push((file.clone(), e)); // Collect error with filename
                }
            }

            let path = PathBuf::from(&raw_dir).join(file);
            // Attempt to remove the file, even if upload fails
            if let Err(e) = std::fs::remove_file(path.clone()) {
                eprintln!("Error removing file {}: {}", path.display(), e);
                errors.push((file.clone(), e.into())); // Collect error with filename
            }
        }

        // If there are any errors, return them as a single composite error
        if !errors.is_empty() {
            let error_descriptions: Vec<String> = errors
                .iter()
                .map(|(file, err)| format!("File: {}, Error: {}", file.display(), err))
                .collect();
            let combined_error = format!("Errors occurred:\n{}", error_descriptions.join("\n"));
            return Err(Error::CustomError(combined_error));
        }

        Ok(())
    }

    // async fn upload(&self, client: &Historical, files: Vec<PathBuf>) -> Result<()> {
    //     for file in &files {
    //         let file_string: String = file.to_string_lossy().into_owned();
    //         let response = client.create_mbp_from_file(&file_string).await?;
    //         println!("{:?}", response);
    //         // let _ = load_file(file, &client).await?;
    //     }
    //
    //     Ok(())
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::date_to_unix_nanos;
    use dotenv::dotenv;
    use midas_client::historical::RetrieveParams;
    use serial_test::serial;
    use std::env;
    use time::OffsetDateTime;

    // Set the environment variables for test mode
    const DATASET: Dataset = Dataset::GlbxMdp3;
    const START: OffsetDateTime = time::macros::datetime!(2024-08-20 00:00 UTC);
    const END: OffsetDateTime = time::macros::datetime!(2024-08-20 05:00 UTC);
    const TICKER: &str = "HE.n.0";
    const SCHEMA: Schema = Schema::Mbp1;
    const STYPE: SType = SType::Continuous;
    const FILENAME: &str = "GLBX.MDP3_mbp-1_HE.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn";

    // -- Helper --
    async fn create_test_ticker(ticker: &str) -> Result<()> {
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = midas_client::historical::Historical::new(base_url);

        let first_available = date_to_unix_nanos("2024-08-20")?;
        let instrument = Instrument::new(
            None,
            ticker,
            "Lean hogs",
            mbn::symbols::Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            first_available as u64,
            first_available as u64,
            true,
        );

        client.create_symbol(&instrument).await?;

        Ok(())
    }

    async fn cleanup_test_ticker(ticker: &str) -> Result<()> {
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = midas_client::historical::Historical::new(base_url);
        let id = client.get_symbol(&ticker.to_string()).await?.data;

        let _ = client.delete_symbol(&(id as i32)).await?;

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_update_ticker() -> anyhow::Result<()> {
        assert!(1 == 0);
        Ok(())
    }
    #[tokio::test]
    #[ignore]
    async fn test_update() -> anyhow::Result<()> {
        assert!(1 == 0);
        Ok(())
    }
    #[tokio::test]
    #[serial]
    #[ignore]
    async fn test_download() -> anyhow::Result<()> {
        dotenv().ok();
        let api_key =
            env::var("DATABENTO_KEY").expect("Expected API key in environment variables.");
        let mut databento_vendor = DatabentoVendor::new(&api_key)?;

        // Test
        let tickers = vec![TICKER.to_string()];
        let download_type;
        let path;
        (download_type, path) = databento_vendor
            .download(
                &tickers,
                SCHEMA,
                START,
                END,
                DATASET.as_str(),
                STYPE.as_str(),
                Some("tests/data".to_string()),
            )
            .await?;

        // Validate
        let expected_file = PathBuf::from(FILENAME);
        assert_eq!(path, expected_file);
        assert_eq!(download_type, DownloadType::Stream);

        //Cleanup
        // if path.exists() {
        //     std::fs::remove_file(&path).expect("Failed to delete the test file.");
        // }

        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    // #[ignore]
    async fn test_transform() -> anyhow::Result<()> {
        dotenv().ok();
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = midas_client::historical::Historical::new(base_url);

        let api_key =
            env::var("DATABENTO_KEY").expect("Expected API key in environment variables.");

        let databento_vendor = DatabentoVendor::new(&api_key)?;
        let dbn_file = PathBuf::from(FILENAME);
        let mbn_file = PathBuf::from("test_databento_transform.bin");
        create_test_ticker("HE.n.0").await?;

        // Test
        let path = databento_vendor
            .transform(&dbn_file, &mbn_file, &client, true)
            .await?;

        // Validate
        let check = path.is_file();
        assert_eq!(check, true);

        //Cleanup
        cleanup_test_ticker("HE.n.0").await?;

        if path.exists() {
            std::fs::remove_file(&path).expect("Failed to delete the test file.");
        }

        Ok(())
    }
    #[tokio::test]
    #[serial_test::serial]
    // #[ignore]
    async fn test_stage_stream() -> anyhow::Result<()> {
        dotenv().ok();
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = midas_client::historical::Historical::new(base_url);

        let api_key =
            env::var("DATABENTO_KEY").expect("Expected API key in environment variables.");

        let databento_vendor = DatabentoVendor::new(&api_key)?;
        let download_type = DownloadType::Stream;
        let dbn_file = PathBuf::from(FILENAME);
        let mbn_file = PathBuf::from("test_databento_transform.bin");
        create_test_ticker("HE.n.0").await?;

        // Test
        let files = databento_vendor
            .stage(&download_type, &dbn_file, &mbn_file, &client)
            .await?;

        // Validate
        let processed_dir = env::var("PROCESSED_DIR").expect("PROCESSED_DIR not set.");
        for name in &files {
            let path = PathBuf::from(&processed_dir).join(name);

            let check = path.is_file();
            assert_eq!(check, true);
        }

        //Cleanup
        cleanup_test_ticker("HE.n.0").await?;
        for name in &files {
            let path = PathBuf::from(&processed_dir).join(name);

            if path.exists() {
                std::fs::remove_file(&path).expect("Failed to delete the test file.");
            }
        }

        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    // #[ignore]
    async fn test_stage_batch() -> anyhow::Result<()> {
        dotenv().ok();
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = midas_client::historical::Historical::new(base_url);

        let api_key =
            env::var("DATABENTO_KEY").expect("Expected API key in environment variables.");

        let databento_vendor = DatabentoVendor::new(&api_key)?;
        let download_type = DownloadType::Batch;
        let dbn_file = PathBuf::from(
            "batch_GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
        );
        let mbn_file = PathBuf::from("test_databento_transform.bin");
        create_test_ticker("ZM.n.0").await?;
        create_test_ticker("GC.n.0").await?;

        // Test
        let files = databento_vendor
            .stage(&download_type, &dbn_file, &mbn_file, &client)
            .await?;

        // Validate
        let processed_dir = env::var("PROCESSED_DIR").expect("PROCESSED_DIR not set.");
        for name in &files {
            let path = PathBuf::from(&processed_dir).join(name);

            let check = path.is_file();
            assert_eq!(check, true);
        }

        //Cleanup
        cleanup_test_ticker("ZM.n.0").await?;
        cleanup_test_ticker("GC.n.0").await?;
        for name in &files {
            let path = PathBuf::from(&processed_dir).join(name);

            if path.exists() {
                std::fs::remove_file(&path).expect("Failed to delete the test file.");
            }
        }

        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    // #[ignore]
    async fn test_load() -> anyhow::Result<()> {
        dotenv().ok();
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = midas_client::historical::Historical::new(base_url);

        let api_key =
            env::var("DATABENTO_KEY").expect("Expected API key in environment variables.");

        let databento_vendor = DatabentoVendor::new(&api_key)?;
        let download_type = DownloadType::Stream;
        let dbn_file = PathBuf::from(FILENAME);
        let mbn_file = PathBuf::from("test_databento_transform.bin");
        let ticker = "HE.n.0";
        create_test_ticker(ticker).await?;

        let paths = databento_vendor
            .stage(&download_type, &dbn_file, &mbn_file, &client)
            .await?;

        // Test
        let _ = databento_vendor.upload(&client, paths.clone()).await?;

        // Validate
        let tickers = vec![TICKER.to_string()];
        let params = RetrieveParams::new(tickers, "2024-08-20", "2024-08-21", "mbp-1")?;
        let response = client.get_records(&params).await?;
        assert!(response.data.len() > 0);

        //Cleanup
        cleanup_test_ticker(ticker).await?;

        let processed_dir = env::var("PROCESSED_DIR").expect("PROCESSED_DIR not set.");
        for name in paths {
            let path = PathBuf::from(&processed_dir).join(name);

            if path.exists() {
                std::fs::remove_file(&path).expect("Failed to delete the test file.");
            }
        }

        Ok(())
    }
}
