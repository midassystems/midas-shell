use crate::cli::ProcessCommand;
use crate::context::Context;
use crate::error::Result;
use async_trait::async_trait;
use clap::Args;
use midas_client::historical::RetrieveParams;
use std::fmt::Debug;

#[derive(Debug, Args)]
pub struct HistoricalArgs {
    /// Symbols to download data for.
    #[arg(long, value_delimiter = ',')]
    pub symbols: Vec<String>,

    /// Start date in YYYY-MM-DD HH:MM:SS format.
    #[arg(long)]
    pub start: String,

    /// End date in YYYY-MM-DD HH:MM:SS format.
    #[arg(long)]
    pub end: String,

    /// Schema ex. Mbp1, Ohlcv
    #[arg(long)]
    pub schema: String,

    /// File path to save the downloaded binary data.
    #[arg(long)]
    pub file_path: String,
}

#[async_trait]
impl ProcessCommand for HistoricalArgs {
    async fn process_command(&self, context: &Context) -> Result<()> {
        let client = context.get_historical_client();

        // Attempt to create RetrieveParams and handle errors gracefully
        let params =
            match RetrieveParams::new(self.symbols.clone(), &self.start, &self.end, &self.schema) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("Error: {}", e);
                    return Ok(()); // Return Ok(()) to prevent crash or timeout
                }
            };

        // Attempt to get records and save them to the file
        match client.get_records_to_file(&params, &self.file_path).await {
            Ok(_) => {
                println!("Data successfully saved to {}", self.file_path);
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }

        Ok(())
    }
}
