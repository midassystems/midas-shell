use super::utils::databento_file_name;
use crate::pipeline::vendors::DownloadType;
use crate::utils::user_input;
use crate::Result;
use databento::{
    dbn::{self, Dataset, SType, Schema},
    historical::batch::{DownloadParams, JobState, ListJobsParams, SubmitJobParams},
    historical::metadata::GetBillableSizeParams,
    historical::metadata::GetCostParams,
    historical::timeseries::{GetRangeParams, GetRangeToFileParams},
    HistoricalClient,
};
use std::path::PathBuf;
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
        approval: bool,
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

        if !approval {
            let proceed = user_input()?;
            if proceed == false {
                return Ok(None);
            }
            println!("Operation is continuing...");
        }

        // Dynamic load based on size
        let download_type;
        let file_path;
        let file_name;

        if size < 5.0 {
            println!("Download size is {}GB : Stream Downloading.", size);
            download_type = DownloadType::Stream;
            file_name = databento_file_name(&dataset, &schema, &start, &end, &symbols, false)?;
            file_path = dir_path.join("databento").join(file_name.clone());

            // Ensure the directory exists
            if let Some(parent_dir) = file_path.parent() {
                std::fs::create_dir_all(parent_dir)?;
            }

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

            // Ensure the directory exists
            if let Some(parent_dir) = file_path.parent() {
                std::fs::create_dir_all(parent_dir)?;
            }

            let _ = self
                .fetch_historical_batch_to_file(
                    &dataset, &start, &end, &symbols, &schema, &stype, &file_path,
                )
                .await?;
        }
        println!("Dbn file path : {:?}", file_path);

        Ok(Some((download_type, file_name)))
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
                false,
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
