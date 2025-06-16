use super::super::Datasets;
use crate::commands::TaskManager;
use crate::error::{Error, Result};
use clap::{Args, Subcommand};
use dbn;
use inquire::Confirm;
use inquire::{DateSelect, Select, Text};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::Mutex;

// Data options
struct Dataset;

impl Dataset {
    pub fn list() -> Vec<&'static str> {
        vec![
            "GLBX.MDP3",
            "XNAS.ITCH",
            "XBOS.ITCH",
            "XPSX.ITCH",
            "BATS.PITCH",
            "BATY.PITCH",
            "EDGA.PITCH",
            "EDGX.PITCH",
            "XNYS.PILLAR",
            "XCIS.PILLAR",
            "XASE.PILLAR",
            "XCHI.PILLAR",
            "XCIS.BBO",
            "XCIS.TRADES",
            "MEMX.MEMOIR",
            "EPRL.DOM",
            "OPRA.PILLAR",
            "DBEQ.BASIC",
            "ARCX.PILLAR",
            "IEXG.TOPS",
            "EQUS.PLUS",
            "XNYS.BBO",
            "XNYS.TRADES",
            "XNAS.QBBO",
            "XNAS.NLS",
            "IFEU.IMPACT",
            "NDEX.IMPACT",
            "EQUS.ALL",
            "XNAS.BASIC",
            "EQUS.SUMMARY",
            "XCIS.TRADESBBO",
            "XNYS.TRADESBBO",
            "EQUS.MINI",
        ]
    }
}
struct Schema;

impl Schema {
    pub fn list() -> Vec<&'static str> {
        vec![
            "mbo",
            "mbp-1",
            "mbp-10",
            "tbbo",
            "trades",
            "bbo-1s",
            "bbo-1m",
            "ohlcv-1s",
            "ohlcv-1m",
            "ohlcv-1h",
            "ohlcv-1d",
            "ohlcv-eod",
        ]
    }
}

struct SType;

impl SType {
    pub fn list() -> Vec<&'static str> {
        vec![
            "instrument_id",
            "raw_symbol",
            "smart",
            "continuous",
            "parent",
        ]
    }
}

#[derive(Debug, Args)]
pub struct DatabentoArgs {
    #[command(subcommand)]
    pub subcommand: DatabentoCommands,
}

#[derive(Debug, Subcommand)]
pub enum DatabentoCommands {
    Download,
    Transform,
    Compare,
}

impl DatabentoCommands {
    pub async fn process_command(&self, context: Arc<Mutex<TaskManager>>) -> Result<()> {
        match self {
            DatabentoCommands::Download => {
                let symbols = Text::new("Symbols:").prompt()?;

                let symbols = symbols
                    .split_terminator(",")
                    .map(|opt| opt.to_string())
                    .collect::<Vec<String>>(); // Split by newline characters

                let mut start = DateSelect::new("Start Date:").prompt()?.to_string();
                let mut end = DateSelect::new("End Date:").prompt()?.to_string();
                println!("Date {:?}", start);

                // Try to parse the datetime string as an OffsetDateTime
                start = format!("{}T00:00:00Z", start);
                let start_date = OffsetDateTime::parse(&start, &Rfc3339).map_err(|_| {
                    Error::DateError(
                        "Error: Invalid start date format. Expected format: YYYY-MM-DD".to_string(),
                    )
                })?;

                // Try to parse the datetime string as an OffsetDateTime
                end = format!("{}T00:00:00Z", end);
                let end_date = OffsetDateTime::parse(&end, &Rfc3339).map_err(|_| {
                    Error::DateError(
                        "Error: Invalid end date format. Expected format: YYYY-MM-DD".to_string(),
                    )
                })?;

                let schema =
                    dbn::Schema::from_str(Select::new("Schema:", Schema::list()).prompt()?)?;
                let stype = dbn::SType::from_str(Select::new("Stype:", SType::list()).prompt()?)?;
                let dataset =
                    dbn::Dataset::from_str(Select::new("Dataset:", Dataset::list()).prompt()?)?;

                let approval = Confirm::new("Approval on download : ")
                    .with_default(false)
                    .prompt()?;

                let dir_path = Text::new("File Path:").prompt()?;

                context
                    .lock()
                    .await
                    .download(
                        &symbols,
                        &schema,
                        &dataset,
                        &stype,
                        start_date,
                        end_date,
                        approval,
                        Some(dir_path),
                    )
                    .await;
            }
            DatabentoCommands::Transform => {
                let dataset = mbinary::enums::Dataset::from_str(
                    &Select::new("Dataset:", Datasets::list())
                        .prompt()?
                        .to_lowercase(),
                )?;

                let dbn_filepath = PathBuf::from(Text::new("File Path:").prompt()?);
                let midas_filepath = PathBuf::from(Text::new("File Path:").prompt()?);

                context
                    .lock()
                    .await
                    .transform(dataset, dbn_filepath, midas_filepath)
                    .await;
            }
            DatabentoCommands::Compare => {
                let dbn_filepath = PathBuf::from(Text::new("File Path:").prompt()?);
                let midas_filepath = PathBuf::from(Text::new("File Path:").prompt()?);
                context
                    .lock()
                    .await
                    .dbn_compare(dbn_filepath, midas_filepath)
                    .await;
            }
        }
        Ok(())
    }
}
