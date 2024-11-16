use crate::Result;
use databento::dbn::{Dataset, Schema};
use std::path::PathBuf;
use time::{self, OffsetDateTime};

/// Create the file path for the raw download from databento, including symbols in the file name.
pub fn databento_file_path(
    dir_path: &PathBuf,
    dataset: &Dataset,
    schema: &Schema,
    start: &OffsetDateTime,
    end: &OffsetDateTime,
    symbols: &Vec<String>,
) -> Result<PathBuf> {
    // Join symbols with an underscore to include in the file name
    let symbols_str = symbols.join("_");

    let file_path = dir_path.join(format!(
        "{}_{}_{}_{}_{}.dbn",
        dataset.as_str(),
        schema.as_str(),
        symbols_str,
        start.format(&time::format_description::well_known::Rfc3339)?,
        end.format(&time::format_description::well_known::Rfc3339)?
    ));

    Ok(file_path)
}
