use crate::commands::TaskManager;
use crate::error::{Error, Result};
use crate::shell::commands::Datasets;
use clap::{Args, Subcommand};
use dbn;
use inquire::Confirm;
use inquire::{DateSelect, Select, Text};
use mbinary::enums::Dataset;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::Mutex;

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

                let start = DateSelect::new("Start Date:").prompt()?.to_string();
                let end = DateSelect::new("End Date:").prompt()?.to_string();

                // Try to parse the datetime string as an OffsetDateTime
                let start_date = OffsetDateTime::parse(&start, &Rfc3339).map_err(|_| {
                    Error::DateError(
                        "Error: Invalid start date format. Expected format: YYYY-MM-DD".to_string(),
                    )
                })?;

                // Try to parse the datetime string as an OffsetDateTime
                let end_date = OffsetDateTime::parse(&end, &Rfc3339).map_err(|_| {
                    Error::DateError(
                        "Error: Invalid start date format. Expected format: YYYY-MM-DD".to_string(),
                    )
                })?;
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
                let approval = Confirm::new("Approval on download : ")
                    .with_default(false)
                    .with_help_message("This data is stored for good reasons")
                    .prompt()?;

                let dir_path = Text::new("File Path:").prompt()?;

                context
                    .lock()
                    .await
                    .download(
                        &symbols,
                        &schema_enum,
                        &dataset_enum,
                        &stype_enum,
                        start_date,
                        end_date,
                        approval,
                        Some(dir_path),
                    )
                    .await;
            }
            DatabentoCommands::Transform => {
                let dataset = Dataset::from_str(
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
