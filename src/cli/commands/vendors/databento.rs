use crate::commands::TaskManager;
use crate::error;
use crate::error::{Error, Result};
use crate::vendors::DownloadType;
use clap::{Args, Subcommand};
use dbn;
use mbinary::enums::Dataset;
use std::path::PathBuf;
use std::str::FromStr;
use time::{format_description::well_known::Rfc3339, macros::time, OffsetDateTime};

fn process_start_date(start: &String) -> Result<OffsetDateTime> {
    // Append T00:00 to make it full day at 00:00 UTC
    let start_datetime = format!("{}T00:00:00Z", start);

    // Try to parse the datetime string as an OffsetDateTime
    let start_date = OffsetDateTime::parse(&start_datetime, &Rfc3339).map_err(|_| {
        Error::DateError(
            "Error: Invalid start date format. Expected format: YYYY-MM-DD".to_string(),
        )
    })?;

    Ok(start_date)
}

fn processs_end_date(end: Option<String>) -> Result<OffsetDateTime> {
    let end_date = end
        .as_ref()
        .map(|s| s.clone()) // Clone the string if it exists
        .unwrap_or_else(|| {
            let now = OffsetDateTime::now_utc();
            let end_of_today = now.replace_time(time!(00:00));
            end_of_today.date().to_string() // Return the date part only
        });

    // Append T00:00 to make it full day at 00:00 UTC
    let end_datetime = format!("{}T00:00:00Z", end_date);
    let end_date = OffsetDateTime::parse(&end_datetime, &Rfc3339).map_err(|_| {
        Error::DateError(
            "Error: Invalid start date format. Expected format: YYYY-MM-DD".to_string(),
        )
    })?;
    Ok(end_date)
}

#[derive(Debug, Args)]
pub struct DatabentoArgs {
    #[command(subcommand)]
    pub subcommand: DatabentoCommands,
}

#[derive(Debug, Subcommand)]
pub enum DatabentoCommands {
    /// Standard update, adds mbp for tickers already in the database for entire previous day.
    Update {
        /// Schema ex. Mbp1, Ohlcv
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        approval: bool,
    },
    /// Download databento data to file
    Download {
        /// Tickers ex. AAPL,GOOGL,TSLA
        #[arg(long, value_delimiter = ',')]
        tickers: Vec<String>,

        /// Schema ex. Mbp1, Ohlcv
        #[arg(long)]
        schema: String,

        /// Start date in YYYY-MM-DD HH:MM:SS format.
        #[arg(long)]
        dataset: String,

        /// End date in YYYY-MM-DD HH:MM:SS format.
        #[arg(long)]
        stype: String,

        /// Start date in YYYY-MM-DD HH:MM:SS format.
        #[arg(long)]
        start: String,

        /// End date in YYYY-MM-DD HH:MM:SS format.
        #[arg(long)]
        end: String,

        /// Optional path, if not provided will defualt to RAW_DIR variable.
        #[arg(long)]
        dir_path: Option<String>,

        #[arg(long)]
        approval: bool,
    },
    /// Upload a databento file to database
    Transform {
        #[arg(long)]
        dataset: String,

        /// Schema ex. Mbp1, Ohlcv
        #[arg(long)]
        dbn_filepath: String,

        /// File path to save the downloaded binary data.
        #[arg(long)]
        midas_filepath: String,
    },
    /// Upload a databento file to database
    Upload {
        #[arg(long)]
        dataset: String,

        /// Schema ex. Mbp1, Ohlcv
        #[arg(long)]
        dbn_filepath: String,

        /// End date in YYYY-MM-DD HH:MM:SS format.
        #[arg(long)]
        dbn_downloadtype: String,

        /// File path to save the downloaded binary data.
        #[arg(long)]
        midas_filepath: String,
    },
    /// Compare databento and midas data
    Compare {
        #[arg(long)]
        dbn_filepath: String,
        #[arg(long)]
        midas_filepath: String,
    },
}

impl DatabentoCommands {
    pub async fn process_command(&self, context: TaskManager) -> Result<()> {
        match self {
            DatabentoCommands::Update { dataset, approval } => {
                let dataset = Dataset::from_str(dataset)?;
                context.update(dataset, *approval).await;

                Ok(())
            }

            DatabentoCommands::Download {
                tickers,
                start,
                end,
                schema,
                dataset,
                stype,
                dir_path,
                approval,
            } => {
                let start_date = process_start_date(start)?;
                let end_date = processs_end_date(Some(end.clone()))?;
                let schema_enum = dbn::Schema::from_str(schema.as_str())
                    .map_err(|_| error!(CustomError, "Invalid schema : {}", schema.as_str()))?;
                let dataset_enum = dbn::Dataset::from_str(dataset.as_str())
                    .map_err(|_| error!(CustomError, "Invalid dataset : {}", dataset.as_str()))?;
                let stype_enum = dbn::SType::from_str(stype.as_str())
                    .map_err(|_| error!(CustomError, "Invalid stype : {}", stype.as_str()))?;

                context
                    .download(
                        tickers,
                        &schema_enum,
                        &dataset_enum,
                        &stype_enum,
                        start_date,
                        end_date,
                        *approval,
                        dir_path.clone(),
                    )
                    .await;

                Ok(())
            }
            DatabentoCommands::Transform {
                dataset,
                dbn_filepath,
                midas_filepath,
            } => {
                let dbn_filepath = PathBuf::from(dbn_filepath);
                let midas_filepath = PathBuf::from(midas_filepath);
                let dataset = Dataset::from_str(dataset.as_str())
                    .map_err(|_| error!(CustomError, "Invalid dataset : {}", dataset.as_str()))?;

                context
                    .transform(dataset, dbn_filepath, midas_filepath)
                    .await;

                Ok(())
            }
            DatabentoCommands::Upload {
                dataset,
                dbn_filepath,
                dbn_downloadtype,
                midas_filepath,
            } => {
                let dbn_filepath = PathBuf::from(dbn_filepath);
                let midas_filepath = PathBuf::from(midas_filepath);
                let download_type = DownloadType::try_from(dbn_downloadtype.as_str())?;
                let dataset_enum = Dataset::from_str(dataset.as_str())
                    .map_err(|_| error!(CustomError, "Invalid dataset : {}", dataset.as_str()))?;

                context
                    .upload(dataset_enum, &download_type, &dbn_filepath, &midas_filepath)
                    .await;

                Ok(())
            }
            DatabentoCommands::Compare {
                dbn_filepath,
                midas_filepath,
            } => {
                context
                    .dbn_compare(PathBuf::from(dbn_filepath), PathBuf::from(midas_filepath))
                    .await;

                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn test_process_start_date() -> Result<()> {
        let start_str = "2024-01-01".to_string();

        // Test
        let start = process_start_date(&start_str)?;

        // Validate
        assert_eq!(start, datetime!(2024-01-01 0:00:00.0 +00:00:00));

        Ok(())
    }

    #[test]
    fn test_process_end_date() -> Result<()> {
        let end_str = Some("2024-01-01".to_string());

        // Test
        let end = processs_end_date(end_str)?;

        // Validate
        assert_eq!(end, datetime!(2024-01-01 0:00:00.0 +00:00:00));

        Ok(())
    }

    #[test]
    fn test_process_end_date_auto() -> Result<()> {
        let end_str = None;

        // Test
        let end = processs_end_date(end_str)?;

        // Validate
        let now = OffsetDateTime::now_utc();
        let expt_end = now.replace_time(time::macros::time!(00:00));

        assert_eq!(end, expt_end);

        Ok(())
    }
}
