use crate::{Result, TaskManager};
use inquire::{DateSelect, Select, Text};
use mbinary::enums::{Dataset, Schema, Stype};
use mbinary::params::RetrieveParams;
pub struct Commands;
use super::{Datasets, Schemas, Stypes};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct HistoricalArgs;

impl HistoricalArgs {
    pub async fn command(context: Arc<Mutex<TaskManager>>) -> Result<()> {
        let symbols = Text::new("Symbols:").prompt()?;
        let dataset = Dataset::from_str(
            &Select::new("Dataset:", Datasets::list())
                .prompt()?
                .to_lowercase(),
        )?;
        let schema = Schema::from_str(Select::new("Schema:", Schemas::list()).prompt()?)?;
        let stype = Stype::from_str(Select::new("Stype:", Stypes::list()).prompt()?)?;
        let start_date = DateSelect::new("Start Date:").prompt()?.to_string();
        let end_date = DateSelect::new("Start Date:").prompt()?.to_string();
        let file_path = Text::new("File Path:").prompt()?;

        let symbols = symbols
            .split_terminator(",")
            .map(|opt| opt.to_string())
            .collect::<Vec<String>>(); // Split by newline characters

        // Attempt to create RetrieveParams and handle errors gracefully
        let params = RetrieveParams::new(symbols, &start_date, &end_date, schema, dataset, stype)?;

        // Likely want to add a read lock so potentialy move this to anther tread or window if long
        // running
        context
            .lock()
            .await
            .get_historical(params, &file_path)
            .await;

        Ok(())
    }
}
