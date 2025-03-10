use crate::commands::TaskManager;
use crate::error::Result;
use clap::Args;
use mbinary::enums::{Dataset, Schema, Stype};
use mbinary::params::RetrieveParams;
use std::fmt::Debug;
use std::str::FromStr;

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

    /// Dataset ex. Equities, Futures, Option
    #[arg(long)]
    pub dataset: String,

    /// Dataset ex. Equities, Futures, Option
    #[arg(long)]
    pub stype: String,

    /// File path to save the downloaded binary data.
    #[arg(long)]
    pub file_path: String,
}

impl HistoricalArgs {
    pub async fn process_command(&self, context: TaskManager) -> Result<()> {
        // let client = context.lock().await.get_historical_client();
        let schema = Schema::from_str(&self.schema)?;
        let dataset = Dataset::from_str(&self.dataset)?;
        let stype = Stype::from_str(&self.stype)?;

        // Attempt to create RetrieveParams and handle errors gracefully
        let params = RetrieveParams::new(
            self.symbols.clone(),
            &self.start,
            &self.end,
            schema,
            dataset,
            stype,
        )?;

        context.get_historical(params, &self.file_path).await;

        Ok(())
    }
}
