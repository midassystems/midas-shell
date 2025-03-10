pub mod client;
pub mod compare;
pub mod extract;
pub mod transform;
pub mod utils;

use super::super::utils::get_earlier_of_year_end_or_date;
use crate::error;
use crate::vendors::midas::checks::find_duplicates;
use crate::vendors::{DownloadType, Vendor};
use crate::{Error, Result};
use async_trait::async_trait;
use client::DatabentoClient;
use dbn;
use extract::{read_dbn_batch_dir, read_dbn_file};
use mbinary::enums::Dataset;
use mbinary::enums::Schema;
use mbinary::metadata::Metadata;
use mbinary::symbols::{Instrument, SymbolMap};
use mbinary::vendors::{VendorData, Vendors};
use midas_client::historical::Historical;
use midas_client::instrument::Instruments;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use time::{macros::time, OffsetDateTime};
use transform::{instrument_id_map, to_mbinary};

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
        schema: &dbn::Schema,
        stype: &dbn::SType,
        dbn_dataset: &dbn::Dataset,
        mbinary_dataset: &Dataset,
        client: &Historical,
        instrument_client: &Instruments,
        download_approval: bool,
    ) -> Result<()> {
        // Download
        let (download_type, file_name) = self
            .download(
                &vec![ticker.ticker.clone()],
                schema, //  Shoudl alway  be mbp1
                dbn_dataset,
                stype,
                *start,
                *end,
                download_approval,
                None,
            )
            .await?;

        // Mbn file path
        let mbinary_filename = PathBuf::from(format!(
            "{}_{}_{}_{}.bin",
            &ticker.ticker,
            &stype,
            start.date(),
            end.date()
        ));

        // Stage
        let files = self
            .stage(
                // mbinary_map,
                *mbinary_dataset,
                &download_type,
                &file_name,
                &mbinary_filename,
                instrument_client,
            )
            .await?;

        // Upload
        let _ = self.upload(client, files).await?;

        // Update instrument
        Ok(())
    }

    pub async fn download(
        &mut self,
        tickers: &Vec<String>,
        schema: &dbn::Schema,
        dataset: &dbn::Dataset,
        stype: &dbn::SType,
        start: OffsetDateTime,
        end: OffsetDateTime,
        approval: bool,
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
                dataset, // &dbnDataset::from_str(&dataset)?,
                &start, &end, &tickers, &schema, stype, // &SType::from_str(&stype)?,
                &dir, approval,
            )
            .await?
            .ok_or(Error::NoDataError)?;

        Ok((download_type, file_name))
    }
}

#[async_trait]
impl Vendor for DatabentoVendor {
    async fn update(
        &mut self,
        dataset: Dataset,
        hist_client: &Historical,
        instrument_client: &Instruments,
        download_approval: bool,
    ) -> Result<()> {
        // Calculate today at the start of the day once
        let today = OffsetDateTime::now_utc().replace_time(time!(00:00));

        // Get tickers
        let api_response = instrument_client
            .list_vendor_symbols(&Vendors::Databento, &dataset)
            .await?;
        let tickers: Vec<Instrument> = api_response.data;

        // Iterate over different request
        for mut ticker in tickers {
            let mut end_flag = false;
            let vendor_data = ticker.get_vendor_data();

            if let VendorData::Databento(data) = vendor_data {
                let stype: dbn::SType = data.stype;
                let schema: dbn::Schema = data.schema;
                let dbn_dataset: dbn::Dataset = data.dataset;

                while !end_flag {
                    let start = ticker.last_available_datetime()?;
                    let end = get_earlier_of_year_end_or_date(start, today);
                    println!("Ticker {:?} Start {:?} End {:?}", ticker.ticker, start, end);

                    if start == end
                        || (ticker.last_available > ticker.expiration_date
                            && ticker.dataset != Dataset::Equities)
                    {
                        println!("Ticker {:?} is already up-to-date.", ticker.ticker);
                        break; // Move to the next ticker
                    }

                    // Load data
                    if let Err(e) = self
                        .update_ticker(
                            &ticker,
                            &start,
                            &end,
                            &schema,
                            &stype,
                            &dbn_dataset,
                            &dataset,
                            hist_client,
                            instrument_client,
                            download_approval,
                        )
                        .await
                    {
                        if let Error::DatabentoError(databento::Error::Api(api_error)) = &e {
                            if api_error.status_code == 422
                                && api_error
                                    .message
                                    .contains("None of the symbols could be resolved")
                            {
                                println!(
                                    "DatabentoError: Failed to upload ticker {} for start {} and end {}. Reason: {:?}",
                                    ticker.ticker, start, end, api_error.message
                                );
                            }
                        } else {
                            // Re-raise other errors
                            error!(
                                CustomError,
                                "Failed to upload ticker {} for start {} and end {} : {:?}",
                                ticker.ticker,
                                start,
                                end,
                                e
                            );
                            return Err(e); // Propagate other errors
                        }
                    }
                    // Update ticker last_available field
                    let last_available = end.unix_timestamp_nanos() as u64; // end.unix_timstamp_nanos() as u64;
                    ticker.last_available = last_available; // end.unix_timestamp_nanos() as u64;

                    if last_available > ticker.expiration_date && dataset != Dataset::Equities {
                        end_flag = true;
                    }

                    instrument_client
                        .update_symbol(&ticker)
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

                // Do something with stype, schema, dataset...
            } else {
                return Err(Error::CustomError(
                    "Unable to parse vendor_data.".to_string(),
                ));
            }
        }

        Ok(())
    }

    async fn transform(
        &self,
        dataset: Dataset,
        dbn_filename: &PathBuf,
        mbinary_filename: &PathBuf,
        instrument_client: &Instruments,
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
        let mut mbinary_map = HashMap::new();

        for (_id, ticker) in dbn_map.iter() {
            let api_response = instrument_client
                .get_symbol(ticker, &dataset)
                .await
                .map_err(|_| error!(CustomError, "Error getting ticker : {} .", ticker.clone()))?;
            let instrument: &Instrument = &api_response.data[0];
            mbinary_map.insert(instrument.ticker.clone(), instrument.instrument_id.unwrap());
        }

        // -- TRANSFORM
        // Map DBN instrument to MBN insturment
        let mbinary_filepath = if env_dirs {
            let processed_dir = env::var("PROCESSED_DIR").expect("PROCESSED_DIR not set.");
            &PathBuf::from(processed_dir).join(mbinary_filename)
        } else {
            mbinary_filename
        };

        let new_map = instrument_id_map(dbn_map, mbinary_map.clone())?;
        let metadata = Metadata::new(Schema::Mbp1, dataset, 0, 0, SymbolMap::new());
        let _ = to_mbinary(&metadata, &mut records, &new_map, mbinary_filepath).await?;
        let _ = drop(records);

        // Check for duplicates
        let duplicates_count = find_duplicates(mbinary_filepath).await?;

        if duplicates_count > 0 {
            std::fs::remove_file(mbinary_filepath.clone())?;
        }

        println!("Staged data path : {:?}", mbinary_filepath);

        Ok(mbinary_filepath.clone())
    }

    async fn stage(
        &self,
        dataset: Dataset,
        download_type: &DownloadType,
        download_path: &PathBuf,
        mbinary_filename: &PathBuf,
        instrument_client: &Instruments,
    ) -> Result<Vec<PathBuf>> {
        let mut files_list = Vec::new();

        if download_type == &DownloadType::Stream {
            let _ = self
                .transform(
                    dataset,
                    download_path,
                    mbinary_filename,
                    instrument_client,
                    true,
                )
                .await?;
            files_list.push(mbinary_filename.clone());
        } else {
            let raw_dir = env::var("RAW_DIR").expect("RAW_DIR not set.");
            let path = PathBuf::from(&raw_dir)
                .join("databento")
                .join(download_path);
            let files = read_dbn_batch_dir(&path).await?;

            let mut count = 0;
            let processed_dir = env::var("PROCESSED_DIR").expect("PROCESSED_DIR not set.");

            for file in files {
                let mbinary_file = PathBuf::from(format!(
                    "{}_{}",
                    count,
                    mbinary_filename.file_name().unwrap().to_string_lossy()
                ));

                let mbinary_path = PathBuf::from(&processed_dir).join(&mbinary_file);
                let _ = self
                    .transform(dataset, &file, &mbinary_path, &instrument_client, false)
                    .await?;
                files_list.push(mbinary_file);
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
                    println!("Upload : {:?}", response);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::date_to_unix_nanos;
    use dotenv::dotenv;
    use mbinary::enums::Schema;
    use mbinary::params::RetrieveParams;
    use mbinary::vendors::{DatabentoData, VendorData, Vendors};
    use serial_test::serial;
    use std::env;
    use std::str::FromStr;
    use time::OffsetDateTime;

    // Set the environment variables for test mode
    const DATASET: dbn::Dataset = dbn::Dataset::GlbxMdp3;
    const SCHEMA: dbn::Schema = dbn::Schema::Mbp1;
    const STYPE: dbn::SType = dbn::SType::Continuous;
    const START: OffsetDateTime = time::macros::datetime!(2024-08-20 00:00 UTC);
    const END: OffsetDateTime = time::macros::datetime!(2024-08-20 05:00 UTC);
    const TICKER: &str = "HE.n.0";
    const FILENAME: &str =
        "GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn";

    // -- Helper --
    async fn create_test_ticker(ticker: &str) -> Result<u32> {
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = midas_client::instrument::Instruments::new(base_url);

        let first_available = date_to_unix_nanos("2024-08-20", None)?;
        let expiration_date = date_to_unix_nanos("2025-08-20", None)?;
        let schema = dbn::Schema::from_str("mbp-1")?;
        let dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset,
            stype,
        });
        let instrument = Instrument::new(
            None,
            ticker,
            "Lean hogs",
            Dataset::Futures,
            Vendors::Databento,
            vendor_data.encode(),
            first_available as u64,
            first_available as u64,
            expiration_date as u64,
            true,
        );

        let response = client.create_symbol(&instrument).await?;
        let id = response.data;

        Ok(id)
    }

    async fn cleanup_test_ticker(id: u32) -> Result<()> {
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = midas_client::instrument::Instruments::new(base_url);

        // let id = client.get_symbol(&ticker.to_string()).await?.data;

        let _ = client.delete_symbol(&(id as i32)).await?;

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
                &SCHEMA,
                &DATASET,
                &STYPE,
                START,
                END,
                false,
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
        let inst_client = midas_client::instrument::Instruments::new(base_url);

        let api_key =
            env::var("DATABENTO_KEY").expect("Expected API key in environment variables.");

        let databento_vendor = DatabentoVendor::new(&api_key)?;
        let dbn_file = PathBuf::from(FILENAME);
        let mbinary_file = PathBuf::from("test_databento_transform.bin");
        let mut ids = Vec::new();
        ids.push(create_test_ticker("GC.n.0").await?);
        ids.push(create_test_ticker("ZM.n.0").await?);

        // Test
        let path = databento_vendor
            .transform(
                Dataset::Futures,
                &dbn_file,
                &mbinary_file,
                &inst_client,
                true,
            )
            .await?;

        // Validate
        let check = path.is_file();
        assert_eq!(check, true);

        //Cleanup
        for id in ids {
            cleanup_test_ticker(id).await?;
        }

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
        let inst_client = midas_client::instrument::Instruments::new(base_url);

        let api_key =
            env::var("DATABENTO_KEY").expect("Expected API key in environment variables.");

        let databento_vendor = DatabentoVendor::new(&api_key)?;
        let download_type = DownloadType::Stream;
        let dbn_file = PathBuf::from(FILENAME);
        let mbinary_file = PathBuf::from("test_databento_transform.bin");
        let mut ids = Vec::new();
        ids.push(create_test_ticker("GC.n.0").await?);
        ids.push(create_test_ticker("ZM.n.0").await?);

        // Test
        let files = databento_vendor
            .stage(
                Dataset::Futures,
                &download_type,
                &dbn_file,
                &mbinary_file,
                &inst_client,
            )
            .await?;

        // Validate
        let processed_dir = env::var("PROCESSED_DIR").expect("PROCESSED_DIR not set.");
        for name in &files {
            let path = PathBuf::from(&processed_dir).join(name);

            let check = path.is_file();
            assert_eq!(check, true);
        }

        //Cleanup
        for id in ids {
            cleanup_test_ticker(id).await?;
        }

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
        let inst_client = midas_client::instrument::Instruments::new(base_url);

        let api_key =
            env::var("DATABENTO_KEY").expect("Expected API key in environment variables.");

        let databento_vendor = DatabentoVendor::new(&api_key)?;
        let download_type = DownloadType::Batch;
        let dbn_file = PathBuf::from(
            "batch_GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
        );
        let mbinary_file = PathBuf::from("test_databento_transform.bin");
        let mut ids = Vec::new();
        ids.push(create_test_ticker("ZM.n.0").await?);
        ids.push(create_test_ticker("GC.n.0").await?);

        // Test
        let files = databento_vendor
            .stage(
                Dataset::Futures,
                &download_type,
                &dbn_file,
                &mbinary_file,
                &inst_client,
            )
            .await?;

        // Validate
        let processed_dir = env::var("PROCESSED_DIR").expect("PROCESSED_DIR not set.");
        for name in &files {
            let path = PathBuf::from(&processed_dir).join(name);

            let check = path.is_file();
            assert_eq!(check, true);
        }

        //Cleanup
        for id in ids {
            cleanup_test_ticker(id).await?;
        }

        // cleanup_test_ticker("GC.n.0").await?;
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
        let inst_client = midas_client::instrument::Instruments::new(base_url);
        let hist_client = midas_client::historical::Historical::new(base_url);

        let api_key =
            env::var("DATABENTO_KEY").expect("Expected API key in environment variables.");

        let databento_vendor = DatabentoVendor::new(&api_key)?;
        let download_type = DownloadType::Stream;
        let dbn_file = PathBuf::from(FILENAME);
        let mbinary_file = PathBuf::from("test_databento_transform.bin");
        let mut ids = Vec::new();
        ids.push(create_test_ticker("ZM.n.0").await?);
        ids.push(create_test_ticker("GC.n.0").await?);

        let paths = databento_vendor
            .stage(
                Dataset::Futures,
                &download_type,
                &dbn_file,
                &mbinary_file,
                &inst_client,
            )
            .await?;

        // Test
        let _ = databento_vendor.upload(&hist_client, paths.clone()).await?;

        // Validate
        let tickers = vec![TICKER.to_string()];
        let params = RetrieveParams::new(
            tickers,
            "2024-08-20",
            "2024-08-21",
            Schema::Mbp1,
            Dataset::Futures,
            mbinary::enums::Stype::Raw,
        )?;
        let response = hist_client.get_records(&params).await?;
        assert!(response.data.len() > 0);

        //Cleanup
        for id in ids {
            cleanup_test_ticker(id).await?;
        }

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
