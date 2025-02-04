use crate::context::Context;

use crate::error::{Error, Result};
use crate::{cli::ProcessCommand, utils::date_to_unix_nanos};
use async_trait::async_trait;
use clap::{Args, Subcommand};
use databento::dbn;
use mbinary::enums::Dataset;
use mbinary::symbols::Instrument;
use mbinary::vendors::{DatabentoData, VendorData, Vendors}; //, YfinanceData};
use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;

fn parse_vendor_data(s: &str) -> Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    for entry in s.split(',') {
        let parts: Vec<&str> = entry.split('=').collect();
        if parts.len() == 2 {
            map.insert(parts[0].to_string(), parts[1].to_string());
        } else {
            return Err(Error::CustomError(format!(
                "Invalid key-value pair: {}",
                entry
            )));
        }
    }
    Ok(map)
}

fn construct_vendor_data(vendor: &Vendors, data: HashMap<String, String>) -> Result<VendorData> {
    match vendor {
        Vendors::Databento => {
            // Extract required values or raise an error if missing
            let stype = data
                .get("stype")
                .ok_or_else(|| Error::CustomError("Missing required 'stype'".to_string()))?;
            let schema = data
                .get("schema")
                .ok_or_else(|| Error::CustomError("Missing required 'schema'".to_string()))?;
            let dataset = data
                .get("dataset")
                .ok_or_else(|| Error::CustomError("Missing required 'database'".to_string()))?;

            // Convert `stype` to the correct type (e.g., dbn::SType)
            let stype_enum = dbn::SType::from_str(stype)
                .map_err(|_| Error::CustomError(format!("Invalid 'stype': {}", stype)))?;
            let schema_enum = dbn::Schema::from_str(schema)
                .map_err(|_| Error::CustomError(format!("Invalid 'schema': {}", stype)))?;
            let dataset_enum = dbn::Dataset::from_str(dataset)
                .map_err(|_| Error::CustomError(format!("Invalid 'dataset': {}", stype)))?;

            Ok(VendorData::Databento(DatabentoData {
                stype: stype_enum,
                schema: schema_enum,
                dataset: dataset_enum,
            }))
        } // Vendors::Yfinance => {
        //     // Extract required values or raise an error if missing
        //     let schema = data
        //         .get("schema")
        //         .ok_or_else(|| Error::CustomError("Missing required 'schema'".to_string()))?;
        //
        //     // Convert `stype` to the correct type (e.g., dbn::SType)
        //     let schema_enum = dbn::Schema::from_str(stype)
        //         .map_err(|_| Error::CustomError(format!("Invalid 'schema': {}", stype)))?;
        //
        //     Ok(VendorData::Yfinance(YfinanceData {
        //         schema: schema_enum,
        //     }))
        // }
        _ => Err(Error::CustomError("Vendor not implemeted".to_string())),
    }
}

#[derive(Debug, Args)]
pub struct InstrumentArgs {
    #[command(subcommand)]
    pub subcommand: InstrumentCommands,
}

#[derive(Debug, Subcommand)]
pub enum InstrumentCommands {
    Create(CreateArgs),
    Get(GetArgs),
    Delete(DeleteArgs),
    Update(UpdateArgs),
}

#[derive(Debug, Args)]
pub struct CreateArgs {
    /// Instrument ticker.
    #[arg(long)]
    pub ticker: String,
    /// Instrument name.
    #[arg(long)]
    pub name: String,
    // Vendor specific
    #[arg(long)]
    pub dataset: String,
    /// Vendor Name
    #[arg(long)]
    pub vendor: String,
    /// Vendor-specific data (key-value pairs)
    #[arg(long, value_parser = parse_vendor_data)]
    pub vendor_data: HashMap<String, String>,
    /// first date available in database
    #[arg(long)]
    pub first_available: String,
    /// last date available in database
    #[arg(long)]
    pub expiration_date: String,
    /// Active status
    #[arg(long)]
    pub active: bool,
}

#[derive(Debug, Args)]
pub struct GetArgs {
    /// Dataset Filter
    #[arg(long)]
    pub dataset: String,
    /// Vendor Filter
    #[arg(long)]
    pub vendor: Option<String>,
}

#[derive(Debug, Args)]
pub struct UpdateArgs {
    /// Instrument ticker.
    #[arg(long)]
    pub instrument_id: i32,
    /// Instrument ticker.
    #[arg(long)]
    pub ticker: String,
    /// Instrument name.
    #[arg(long)]
    pub name: String,
    // Vendor Specific
    #[arg(long)]
    pub dataset: String,
    /// Vendor Name
    #[arg(long)]
    pub vendor: String,
    /// Vendor-specific data (key-value pairs)
    #[arg(long, value_parser = parse_vendor_data)]
    pub vendor_data: HashMap<String, String>,
    /// first date available in database
    #[arg(long)]
    pub first_available: String,
    /// last date available in database
    #[arg(long)]
    pub last_available: String,
    /// last date available in database
    #[arg(long)]
    pub expiration_date: String,
    /// Active status
    #[arg(long)]
    pub active: bool,
}

#[derive(Debug, Args)]
pub struct DeleteArgs {
    /// Vendor Filter
    #[arg(long)]
    pub instrument_id: i32,
}
#[async_trait]
impl ProcessCommand for InstrumentCommands {
    async fn process_command(&self, context: &Context) -> Result<()> {
        let client = context.get_instrument_client();

        match self {
            InstrumentCommands::Create(args) => {
                let vendor = Vendors::from_str(&args.vendor)?;
                let dataset = Dataset::from_str(&args.dataset)?;
                let first_available = date_to_unix_nanos(&args.first_available, None)?;
                let expiration_date =
                    date_to_unix_nanos(&args.expiration_date, Some("America/New_York"))?;

                let vendor_data_map = args.vendor_data.clone();
                let vendor_data = construct_vendor_data(&vendor, vendor_data_map).map_err(|e| {
                    Error::CustomError(format!("Failed to parse vendor data '{}'", e))
                })?;

                let instrument = Instrument::new(
                    None,
                    &args.ticker,
                    &args.name,
                    dataset,
                    vendor,
                    vendor_data.encode(),
                    first_available as u64,
                    first_available as u64,
                    expiration_date as u64,
                    args.active,
                );

                match client.create_symbol(&instrument).await {
                    Ok(_) => {
                        println!("Created symbol successfully.");
                    }
                    Err(e) => {
                        eprintln!("{}", e);
                    }
                }
            }
            InstrumentCommands::Get(args) => {
                let response;
                let dataset = Dataset::from_str(&args.dataset)?;

                if args.vendor.is_some() {
                    let vendor = Vendors::from_str(args.vendor.as_ref().unwrap())?;
                    response = client.list_vendor_symbols(&vendor, &dataset).await;
                } else {
                    response = client.list_dataset_symbols(&dataset).await;
                }

                match response {
                    Ok(symbols) => {
                        println!("{:?}", symbols.data);
                    }
                    Err(e) => {
                        eprintln!("{}", e);
                    }
                }
            }
            InstrumentCommands::Delete(args) => {
                match client.delete_symbol(&args.instrument_id).await {
                    Ok(_) => {
                        println!("Successfully deleted instrument.");
                    }
                    Err(e) => {
                        eprintln!("{}", e);
                    }
                }
            }
            InstrumentCommands::Update(args) => {
                let vendor = Vendors::from_str(&args.vendor)?;
                let dataset = Dataset::from_str(&args.dataset)?;
                let first_available = date_to_unix_nanos(&args.first_available, None)?;
                let last_available = date_to_unix_nanos(&args.last_available, None)?;
                let expiration_date =
                    date_to_unix_nanos(&args.expiration_date, Some("America/New_York"))?;
                let vendor_data_map = args.vendor_data.clone();
                let vendor_data = construct_vendor_data(&vendor, vendor_data_map).map_err(|e| {
                    Error::CustomError(format!("Failed to parse vendor data '{}'", e))
                })?;

                let instrument = Instrument::new(
                    Some(args.instrument_id as u32),
                    &args.ticker,
                    &args.name,
                    dataset,
                    vendor,
                    vendor_data.encode(),
                    first_available as u64,
                    last_available as u64,
                    expiration_date as u64,
                    args.active,
                );
                match client.update_symbol(&instrument).await {
                    Ok(_) => {
                        println!("Updated symbol successfully.");
                    }
                    Err(e) => {
                        eprintln!("{}", e);
                    }
                }
            }
        }

        Ok(())
    }
}
