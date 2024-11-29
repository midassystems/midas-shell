use crate::context::Context;
use crate::error::Result;
use crate::{cli::ProcessCommand, utils::date_to_unix_nanos};
use async_trait::async_trait;
use clap::{Args, Subcommand};
use mbn::symbols::Instrument;
use std::fmt::Debug;

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
    /// Vendor Name
    #[arg(long)]
    pub vendor: String,
    // Vendor Specific
    #[arg(long)]
    pub stype: Option<String>,
    // Vendor specific
    #[arg(long)]
    pub dataset: Option<String>,
    /// first date available in database
    #[arg(long)]
    pub first_available: String,
    /// Active status
    #[arg(long)]
    pub active: bool,
}

#[derive(Debug, Args)]
pub struct GetArgs {
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
    /// Vendor Name
    #[arg(long)]
    pub vendor: String,
    // Vendor Specific
    #[arg(long)]
    pub stype: Option<String>,
    // Vendor specific
    #[arg(long)]
    pub dataset: Option<String>,
    /// first date available in database
    #[arg(long)]
    pub first_available: String,
    /// last date available in database
    #[arg(long)]
    pub last_available: String,
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
        let client = context.get_historical_client();
        match self {
            InstrumentCommands::Create(args) => {
                let vendor = mbn::symbols::Vendors::try_from(args.vendor.as_str())?;

                let first_available = date_to_unix_nanos(&args.first_available)?;
                // let last_available = date_to_unix_na(available)?;

                let instrument = Instrument::new(
                    None,
                    &args.ticker,
                    &args.name,
                    vendor,
                    args.stype.clone(),
                    args.dataset.clone(),
                    first_available as u64,
                    first_available as u64,
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

                if args.vendor.is_some() {
                    let vendor = args.vendor.as_ref().unwrap();
                    response = client.list_vendor_symbols(vendor).await;
                } else {
                    response = client.list_symbols().await;
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
                let vendor = mbn::symbols::Vendors::try_from(args.vendor.as_str())?;
                let first_available = date_to_unix_nanos(&args.first_available)?;
                let last_available = date_to_unix_nanos(&args.last_available)?;

                let instrument = Instrument::new(
                    None,
                    &args.ticker,
                    &args.name,
                    vendor,
                    args.stype.clone(),
                    args.dataset.clone(),
                    first_available as u64,
                    last_available as u64,
                    args.active,
                );
                match client.update_symbol(&instrument, &args.instrument_id).await {
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
