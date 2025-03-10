use super::{Datasets, Vendor};
use crate::commands::TaskManager;
use crate::error::{Error, Result};
use crate::utils::date_to_unix_nanos;
use clap::{Args, Subcommand};
use dbn;
use inquire::{Confirm, CustomType};
use inquire::{DateSelect, Select, Text};
use mbinary::enums::Dataset;
use mbinary::symbols::Instrument;
use mbinary::vendors::{DatabentoData, VendorData, Vendors};
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;

//
// fn get_date(name: &str) -> InquireResult<i64> {
//     let naive_date = DateSelect::new(&format!("{} Date:", name)).prompt()?;
//     let naive_datetime = naive_date.and_hms_opt(0, 0, 0).unwrap();
//
//     // Convert NaiveDateTime to DateTime<Utc> (UTC timezone-aware)
//     let utc_datetime: DateTime<Utc> = Utc.from_utc_datetime(&naive_datetime);
//     let unix_nanos = utc_datetime.timestamp_nanos_opt().unwrap();
//     Ok(unix_nanos)
// }

fn construct_vendor_data(vendor: &Vendors) -> Result<VendorData> {
    match vendor {
        Vendors::Databento => {
            let stype = Text::new("Stype:").prompt()?;
            let schema = Text::new("Schema:").prompt()?;
            let dataset = Text::new("Dataset:").prompt()?;

            // Convert `stype` to the correct type (e.g., dbn::SType)
            let stype_enum = dbn::SType::from_str(&stype)
                .map_err(|_| Error::CustomError(format!("Invalid 'stype': {}", stype)))?;
            let schema_enum = dbn::Schema::from_str(&schema)
                .map_err(|_| Error::CustomError(format!("Invalid 'schema': {}", stype)))?;
            let dataset_enum = dbn::Dataset::from_str(&dataset)
                .map_err(|_| Error::CustomError(format!("Invalid 'dataset': {}", stype)))?;

            Ok(VendorData::Databento(DatabentoData {
                stype: stype_enum,
                schema: schema_enum,
                dataset: dataset_enum,
            }))
        }
        _ => Err(Error::CustomError("Vendor not implemeted".to_string())),
    }
}

pub fn create_instrument() -> Result<Instrument> {
    let ticker = Text::new("Ticker:").prompt()?;
    let name = Text::new("Name:").prompt()?;
    let dataset = Dataset::from_str(
        &Select::new("Dataset:", Datasets::list())
            .prompt()?
            .to_lowercase(),
    )?;
    let vendor = Vendors::from_str(
        &Select::new("Vendor:", Vendor::list())
            .prompt()?
            .to_lowercase(),
    )?;

    let vendor_data = construct_vendor_data(&vendor)?;
    let first_available =
        date_to_unix_nanos(&DateSelect::new("Start Date:").prompt()?.to_string(), None)?;
    let expiration_date = date_to_unix_nanos(
        &DateSelect::new("Expiration Date:").prompt()?.to_string(),
        None,
    )?;
    let active = Confirm::new("Set instrument active : ")
        .with_default(false)
        .with_help_message("This data is stored for good reasons")
        .prompt()?;

    Ok(Instrument::new(
        None,
        &ticker,
        &name,
        dataset,
        vendor,
        vendor_data.encode(),
        first_available as u64,
        first_available as u64,
        expiration_date as u64,
        active,
    ))
}

#[derive(Debug, Args)]
pub struct InstrumentArgs {
    #[command(subcommand)]
    pub subcommand: InstrumentCommands,
}

#[derive(Debug, Subcommand)]
pub enum InstrumentCommands {
    Create,
    Get,
    Delete,
}

impl InstrumentCommands {
    pub async fn process_command(&self, context: Arc<Mutex<TaskManager>>) -> Result<()> {
        match self {
            InstrumentCommands::Create => {
                context.lock().await.list_backtest().await;
                let instrument = create_instrument()?;

                context.lock().await.create_instrument(instrument).await;
            }
            InstrumentCommands::Get => {
                let dataset = Dataset::from_str(
                    &Select::new("Dataset:", Datasets::list())
                        .prompt()?
                        .to_lowercase(),
                )?;

                context.lock().await.get_instruments(dataset, None).await;
            }
            InstrumentCommands::Delete => {
                let id: i32 = CustomType::new("Instrument ID:")
                    .with_formatter(&|i: i32| format!("{i}"))
                    .with_error_message("Please type a valid number")
                    .prompt()
                    .unwrap();

                context.lock().await.delete_instrument(id).await;
            }
        }
        Ok(())
    }
}
