pub mod compare;
pub mod extract;
pub mod transform;
pub mod utils;

use crate::error; //Macro
use crate::utils::{load_file, user_input};
use crate::vendors::v_databento::{
    extract::{read_dbn_batch_dir, read_dbn_file},
    transform::{instrument_id_map, to_mbn},
};
use crate::vendors::{DownloadType, Vendor};
use crate::{Error, Result};
use async_trait::async_trait;
use databento::{
    dbn::{self, Dataset, SType, Schema},
    historical::batch::{DownloadParams, JobState, ListJobsParams, SubmitJobParams},
    historical::metadata::GetBillableSizeParams,
    historical::metadata::GetCostParams,
    historical::timeseries::{GetRangeParams, GetRangeToFileParams},
    HistoricalClient,
};
use mbn::symbols::Instrument;
use midas_client::historical::Historical;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use time::{macros::time, OffsetDateTime};
use tokio::io::AsyncReadExt;
use utils::databento_file_name;

#[derive(Debug)]
pub struct DatabentoClient {
    hist_client: HistoricalClient,
}

impl DatabentoClient {
    pub fn new(api_key: &String) -> Result<Self> {
        let hist_client = HistoricalClient::builder().key(api_key)?.build()?;

        Ok(Self { hist_client })
    }

    /// Gets the billable uncompressed raw binary size for historical streaming or batched files.
    async fn check_size(
        &mut self,
        dataset: &Dataset,
        start: &OffsetDateTime,
        end: &OffsetDateTime,
        symbols: &Vec<String>,
        schema: &Schema,
        stype: &SType,
    ) -> Result<f64> {
        let params = GetBillableSizeParams::builder()
            .dataset(*dataset)
            .date_time_range((start.clone(), end.clone()))
            .symbols(symbols.clone())
            .schema(*schema)
            .stype_in(*stype)
            .build();

        let size = self
            .hist_client
            .metadata()
            .get_billable_size(&params)
            .await?;
        let size_gb = size as f64 / 1_000_000_000.0;

        Ok(size_gb)
    }

    /// Gets the cost in US dollars for a historical streaming or batch download request.
    pub async fn check_cost(
        &mut self,
        dataset: &Dataset,
        start: &OffsetDateTime,
        end: &OffsetDateTime,
        symbols: &Vec<String>,
        schema: &Schema,
        stype: &SType,
    ) -> Result<f64> {
        let params = GetCostParams::builder()
            .dataset(*dataset)
            .date_time_range((start.clone(), end.clone()))
            .symbols(symbols.clone())
            .schema(*schema)
            .stype_in(*stype)
            .build();

        let cost = self.hist_client.metadata().get_cost(&params).await?;

        Ok(cost)
    }

    /// Makes a streaming request for timeseries data from Databento and saves to file.
    pub async fn fetch_historical_stream_to_file(
        &mut self,
        dataset: &Dataset,
        start: &OffsetDateTime,
        end: &OffsetDateTime,
        symbols: &Vec<String>,
        schema: &Schema,
        stype: &SType,
        filepath: &PathBuf,
    ) -> Result<dbn::decode::AsyncDbnDecoder<impl AsyncReadExt>> {
        // Define the parameters for the timeseries data request
        let params = GetRangeToFileParams::builder()
            .dataset(*dataset)
            .date_time_range((start.clone(), end.clone()))
            .symbols(symbols.clone())
            .schema(*schema)
            .stype_in(*stype)
            .path(filepath)
            .build();

        let decoder = self
            .hist_client
            .timeseries()
            .get_range_to_file(&params)
            .await?;

        println!("Saved to file.");

        Ok(decoder)
    }

    #[allow(dead_code)]
    pub async fn fetch_historical_stream(
        &mut self,
        dataset: &Dataset,
        start: &OffsetDateTime,
        end: &OffsetDateTime,
        symbols: &Vec<String>,
        schema: &Schema,
        stype: &SType,
    ) -> Result<dbn::decode::AsyncDbnDecoder<impl AsyncReadExt>> {
        // Define the parameters for the timeseries data request
        let params = GetRangeParams::builder()
            .dataset(*dataset)
            .date_time_range((start.clone(), end.clone()))
            .symbols(symbols.clone())
            .schema(*schema)
            .stype_in(*stype)
            .build();

        let decoder = self.hist_client.timeseries().get_range(&params).await?;

        Ok(decoder)
    }

    /// Makes a batch request for timeseries data from Databento and saves to file.
    pub async fn fetch_historical_batch_to_file(
        &mut self,
        dataset: &Dataset,
        start: &OffsetDateTime,
        end: &OffsetDateTime,
        symbols: &Vec<String>,
        schema: &Schema,
        stype: &SType,
        filepath: &PathBuf,
    ) -> Result<()> {
        // Define the parameters for the timeseries data request
        let params = SubmitJobParams::builder()
            .dataset(*dataset)
            .date_time_range((start.clone(), end.clone()))
            .symbols(symbols.clone())
            .schema(*schema)
            .stype_in(*stype)
            .build();

        let job = self.hist_client.batch().submit_job(&params).await?;

        let now = OffsetDateTime::now_utc();
        let list_jobs_query = ListJobsParams::builder()
            .since(now - Duration::from_secs(60))
            .states(vec![JobState::Done])
            .build();
        // Now we wait for the job to complete
        loop {
            let finished_jobs = self.hist_client.batch().list_jobs(&list_jobs_query).await?;
            if finished_jobs.iter().any(|j| j.id == job.id) {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // Once complete, we download the files
        let files = self
            .hist_client
            .batch()
            .download(
                &DownloadParams::builder()
                    .output_dir(filepath)
                    .job_id(job.id)
                    .build(),
            )
            .await?;
        println!("{:?}", files);

        Ok(())
    }

    pub async fn get_historical(
        &mut self,
        dataset: &Dataset,
        start: &OffsetDateTime,
        end: &OffsetDateTime,
        symbols: &Vec<String>,
        schema: &Schema,
        stype: &SType,
        dir_path: &PathBuf,
    ) -> Result<Option<(DownloadType, PathBuf)>> {
        // Cost check
        let cost = self
            .check_cost(&dataset, &start, &end, &symbols, &schema, &stype)
            .await?;

        // Size check
        let size = self
            .check_size(&dataset, &start, &end, &symbols, &schema, &stype)
            .await?;

        // Check with user before proceeding
        println!(
            "Download size is : {} GB.\nEstimated cost is : $ {}\n",
            size, cost
        );
        // println!("The estimated cost for this operation is: $ {}", cost);
        let proceed = user_input()?;
        if proceed == false {
            return Ok(None);
        }
        println!("Operation is continuing...");

        // Dynamic load based on size
        let download_type;
        let file_path;
        let file_name;

        if size < 5.0 {
            println!("Download size is {}GB : Stream Downloading.", size);
            download_type = DownloadType::Stream;
            file_name = databento_file_name(&dataset, &schema, &start, &end, &symbols, false)?;
            file_path = dir_path.join("databento").join(file_name.clone());

            let _ = self
                .fetch_historical_stream_to_file(
                    &dataset, &start, &end, &symbols, &schema, &stype, &file_path,
                )
                .await?;
        } else {
            println!("Download size is {}GB : Batch Downloading", size);
            download_type = DownloadType::Batch;
            file_name = databento_file_name(&dataset, &schema, &start, &end, &symbols, true)?;
            file_path = dir_path.join("databento").join(file_name.clone());

            let _ = self
                .fetch_historical_batch_to_file(
                    &dataset, &start, &end, &symbols, &schema, &stype, &file_path,
                )
                .await?;
        }
        println!("Dbn file path : {:?}", file_path);

        Ok(Some((download_type, file_name)))
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

        // Upload
        let _ = self
            .load(
                // mbn_map,
                &download_type,
                &file_name,
                &mbn_filename,
                client,
            )
            .await?;
        // Update instrument
        Ok(())
    }
}

/// Returns date.year()+1-01-01 00:00 or the alternate date whichever is older
fn get_earlier_of_year_end_or_date(
    date: OffsetDateTime,
    compare_date: OffsetDateTime,
) -> OffsetDateTime {
    // Calculate the start of the next year based on the provided date
    let next_year_start = date
        .replace_date(date.date().replace_year(date.year() + 1).unwrap())
        .replace_time(time!(00:00));

    // Return the earlier of the two dates
    next_year_start.min(compare_date)
}

#[async_trait]
impl Vendor for DatabentoClient {
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
        // mbn_map: &HashMap<String, u32>,
        dbn_filename: &PathBuf,
        mbn_filename: &PathBuf,
        client: &Historical,
    ) -> Result<PathBuf> {
        // -- Extract
        let raw_dir = env::var("RAW_DIR").expect("RAW_DIR not set.");
        let dbn_filepath = &PathBuf::from(raw_dir).join("databento").join(dbn_filename);

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
        let new_map = instrument_id_map(dbn_map, mbn_map.clone())?;
        let processed_dir = env::var("PROCESSED_DIR").expect("PROCESSED_DIR not set.");
        let mbn_filepath = &PathBuf::from(processed_dir).join(mbn_filename);
        let _ = to_mbn(&mut records, &new_map, mbn_filepath).await?;
        let _ = drop(records);
        println!("MBN Path : {:?}", mbn_filepath);

        Ok(mbn_filepath.clone())
    }
    async fn load(
        &self,
        // mbn_map: HashMap<String, u32>,
        download_type: &DownloadType,
        download_path: &PathBuf,
        mbn_filename: &PathBuf,
        client: &Historical,
    ) -> Result<()> {
        if download_type == &DownloadType::Stream {
            let _file = self.transform(download_path, mbn_filename, client).await?;
            load_file(&mbn_filename, client).await?;
        } else {
            let files = read_dbn_batch_dir(download_path).await?;
            for file in files {
                let filepath = self.transform(&file, mbn_filename, client).await?;
                let _ = load_file(&filepath, &client).await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenv::dotenv;
    use serial_test::serial;
    use std::env;
    use std::fs;
    use time::OffsetDateTime;

    fn setup() -> (
        DatabentoClient,
        Dataset,
        OffsetDateTime,
        OffsetDateTime,
        Vec<String>,
        Schema,
        SType,
    ) {
        dotenv().ok();
        let api_key =
            env::var("DATABENTO_KEY").expect("Expected API key in environment variables.");

        // Parameters
        let dataset = Dataset::GlbxMdp3;
        let start = time::macros::datetime!(2024-08-20 00:00 UTC);
        let end = time::macros::datetime!(2024-08-20 05:00 UTC);
        let symbols = vec!["ZM.n.0".to_string(), "GC.n.0".to_string()];
        let schema = Schema::Mbp1;
        let stype = SType::Continuous;

        let client = DatabentoClient::new(&api_key).expect("Failed to create DatabentoClient");
        (client, dataset, start, end, symbols, schema, stype)
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_check_size() {
        let (mut client, dataset, start, end, symbols, schema, stype) = setup();
        let size = client
            .check_size(&dataset, &start, &end, &symbols, &schema, &stype)
            .await
            .expect("Error calculating size");

        assert!(size > 0.0);
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_check_cost() {
        let (mut client, dataset, start, end, symbols, schema, stype) = setup();
        let cost = client
            .check_cost(&dataset, &start, &end, &symbols, &schema, &stype)
            .await
            .expect("Error calculating cost");

        assert!(cost > 0.0);
    }

    #[tokio::test]
    #[serial]
    #[ignore]
    async fn test_stream_to_file() -> anyhow::Result<()> {
        let (mut client, dataset, start, end, symbols, schema, stype) = setup();

        let path = PathBuf::from("tests/data");
        let file_name = databento_file_name(&dataset, &schema, &start, &end, &symbols, false)?;
        let file_path = path.join(file_name);

        let _ = client
            .fetch_historical_stream_to_file(
                &dataset, &start, &end, &symbols, &schema, &stype, &file_path,
            )
            .await
            .expect("Error with stream.");

        assert!(fs::metadata(&file_path).is_ok(), "File does not exist");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    #[ignore]
    async fn test_batch_to_file() -> anyhow::Result<()> {
        let (mut client, dataset, start, end, symbols, schema, stype) = setup();
        let path = PathBuf::from("tests/data");
        let file_name = databento_file_name(&dataset, &schema, &start, &end, &symbols, true)?;
        let file_path = path.join(file_name);

        let _ = client
            .fetch_historical_batch_to_file(
                &dataset, &start, &end, &symbols, &schema, &stype, &file_path,
            )
            .await
            .expect("Error with stream.");

        assert!(fs::metadata(&file_path).is_ok(), "File does not exist");
        Ok(())
    }

    #[tokio::test]
    #[serial]
    #[ignore]
    async fn test_get_historical() -> anyhow::Result<()> {
        let (mut client, dataset, start, end, symbols, schema, stype) = setup();

        // Test
        let result = client
            .get_historical(
                &dataset,
                &start,
                &end,
                &symbols,
                &schema,
                &stype,
                &PathBuf::from("tests/data/databento/get_historical"),
            )
            .await?;

        // Handle the result
        let (download_type, download_path) =
            result.ok_or_else(|| anyhow::anyhow!("No download result"))?;
        println!("{:?}", download_path);

        // Validate
        assert_eq!(download_type, DownloadType::Stream);

        Ok(())
    }
}
