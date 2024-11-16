pub mod compare;
pub mod extract;
pub mod transform;
pub mod utils;

use crate::error::{Error, Result};
use crate::tickers::Ticker;
use crate::utils::{load_file, user_input};
use crate::vendors::v_databento::utils::databento_file_path;
use crate::vendors::v_databento::{
    extract::{read_dbn_batch_dir, read_dbn_file},
    transform::{instrument_id_map, to_mbn},
};
use crate::vendors::{DownloadType, Vendor};
use async_trait::async_trait;
use databento::{
    dbn::{self, Dataset, SType, Schema},
    historical::batch::{DownloadParams, JobState, ListJobsParams, SubmitJobParams},
    historical::metadata::GetBillableSizeParams,
    historical::metadata::GetCostParams,
    historical::timeseries::{GetRangeParams, GetRangeToFileParams},
    HistoricalClient,
};
use midas_client::historical::Historical;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::io::AsyncReadExt;

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

        if size < 5.0 {
            println!("Download size is {}GB : Stream Downloading.", size);
            download_type = DownloadType::Stream;
            file_path = databento_file_path(
                &dir_path.join("databento"),
                &dataset,
                &schema,
                &start,
                &end,
                &symbols,
            )?;

            let _ = self
                .fetch_historical_stream_to_file(
                    &dataset, &start, &end, &symbols, &schema, &stype, &file_path,
                )
                .await?;
        } else {
            println!("Download size is {}GB : Batch Downloading", size);
            download_type = DownloadType::Batch;
            file_path = databento_file_path(
                &dir_path.join("databento/batch"),
                &dataset,
                &schema,
                &start,
                &end,
                &symbols,
            )?;

            let _ = self
                .fetch_historical_batch_to_file(
                    &dataset, &start, &end, &symbols, &schema, &stype, &file_path,
                )
                .await?;
        }

        Ok(Some((download_type, file_path)))
    }
}

// #[async_trait]
impl Vendor for DatabentoClient {
    async fn update<T: AsRef<Historical>>(
        &mut self,
        tickers: Vec<Ticker>,
        client: T,
    ) -> Result<()> {
        // End date
        let now = OffsetDateTime::now_utc();
        let end = now.replace_time(time::macros::time!(00:00));

        // Iterate over different request
        for ticker in tickers {
            let start = ticker.last_update;
            println!(
                "Ticker: {:?} | Start: {:?} => End: {:?}",
                ticker, start, end
            );

            // Download
            let (download_type, download_path) = self
                .download(
                    &vec![ticker.ticker.clone()],
                    Schema::Mbp1,
                    start,
                    end,
                    &ticker.dataset,
                    &ticker.stype,
                )
                .await?;

            // Mbn file path
            let mbn_filename = PathBuf::from(format!(
                "{}_{}_{}_{}.bin",
                &ticker.ticker,
                &ticker.stype,
                start.date(),
                end.date()
            ));

            let mbn_map: HashMap<String, u32> =
                [(ticker.ticker.clone(), ticker.get_mbn_id()?)].into();

            // Upload
            let _ = self
                .load(
                    mbn_map,
                    &download_type,
                    &download_path,
                    &mbn_filename,
                    &client,
                )
                .await?;
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
    ) -> Result<(DownloadType, PathBuf)> {
        // // Create the DatabentoClient
        // let api_key = std::env::var("DATABENTO_KEY").expect("DATABENTO_KEY not set.");
        // let mut client = DatabentoClient::new(api_key)?;

        // Download
        let raw_dir = std::env::var("RAW_DIR").expect("RAW_DIR not set.");
        let (download_type, download_path) = self
            .get_historical(
                &Dataset::from_str(&dataset)?,
                &start,
                &end,
                &tickers,
                &schema,
                &SType::from_str(&stype)?,
                &PathBuf::from(raw_dir),
            )
            .await?
            .ok_or(Error::NoDataError)?;
        println!("{:?}", download_path);

        Ok((download_type, download_path))
    }

    async fn transform(
        &self,
        mbn_map: &HashMap<String, u32>,
        dbn_filename: &PathBuf,
        mbn_filename: &PathBuf,
    ) -> Result<PathBuf> {
        // println!("{:?}", dbn_path);
        // -- Extract
        let raw_dir = env::var("RAW_DIR").expect("RAW_DIR not set.");
        let dbn_filepath = &PathBuf::from(raw_dir).join("databento").join(dbn_filename);

        let mut records;
        let dbn_map;
        (records, dbn_map) = read_dbn_file(dbn_filepath.clone()).await?;
        println!("file read");

        // -- TRANSFORM
        // Map DBN instrument to MBN insturment
        let processed_dir = env::var("PROCESSED_DIR").expect("PROCESSED_DIR not set.");
        let mbn_filepath = &PathBuf::from(processed_dir).join(mbn_filename);
        let new_map = instrument_id_map(dbn_map, mbn_map.clone())?;
        let _ = to_mbn(&mut records, &new_map, mbn_filepath).await?;
        let _ = drop(records);
        println!("MBN Path : {:?}", mbn_filepath);

        Ok(mbn_filepath.clone())
    }
    async fn load<T: AsRef<Historical>>(
        &self,
        mbn_map: HashMap<String, u32>,
        download_type: &DownloadType,
        download_path: &PathBuf,
        mbn_filename: &PathBuf,
        client: &T,
    ) -> Result<()> {
        if download_type == &DownloadType::Stream {
            let file = self
                .transform(&mbn_map, download_path, mbn_filename)
                .await?;
            load_file(&mbn_filename, client).await?;
        } else {
            let files = read_dbn_batch_dir(download_path).await?;
            for file in files {
                let filepath = self.transform(&mbn_map, &file, mbn_filename).await?;
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
    async fn test_stream_to_file() {
        let (mut client, dataset, start, end, symbols, schema, stype) = setup();

        let file_path = databento_file_path(
            &PathBuf::from("tests/data/databento"),
            &dataset,
            &schema,
            &start,
            &end,
            &symbols,
        )
        .expect("Error creatign file_path");
        let _ = client
            .fetch_historical_stream_to_file(
                &dataset, &start, &end, &symbols, &schema, &stype, &file_path,
            )
            .await
            .expect("Error with stream.");

        assert!(fs::metadata(&file_path).is_ok(), "File does not exist");
    }

    #[tokio::test]
    #[serial]
    #[ignore]
    async fn test_batch_to_file() {
        let (mut client, dataset, start, end, symbols, schema, stype) = setup();

        let file_path = databento_file_path(
            &PathBuf::from("tests/data/databento/batch"),
            &dataset,
            &schema,
            &start,
            &end,
            &symbols,
        )
        .expect("Error creatign file_path");

        let _ = client
            .fetch_historical_batch_to_file(
                &dataset, &start, &end, &symbols, &schema, &stype, &file_path,
            )
            .await
            .expect("Error with stream.");

        assert!(fs::metadata(&file_path).is_ok(), "File does not exist");
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
