use crate::error::{Error, Result};
use async_compression::tokio::bufread::ZstdDecoder;
use databento::{dbn, historical::timeseries::AsyncDbnDecoder};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::BufReader;
use walkdir::WalkDir;

pub fn symbol_map(metadata: &dbn::Metadata) -> Result<HashMap<String, String>> {
    let mut symbol_map_hash = HashMap::new();

    for mapping in &metadata.mappings {
        for interval in &mapping.intervals {
            symbol_map_hash.insert(interval.symbol.clone(), mapping.raw_symbol.to_string());
        }
    }
    Ok(symbol_map_hash)
}

/// Read stream dbn file.
pub async fn read_dbn_file(
    filepath: PathBuf,
) -> Result<(
    AsyncDbnDecoder<ZstdDecoder<BufReader<File>>>,
    HashMap<String, String>,
)> {
    // Read the file
    let decoder = AsyncDbnDecoder::from_zstd_file(filepath)
        .await
        .map_err(|_| anyhow::anyhow!("Error opeing dbn file."))?;

    // Extract Symbol Map
    let metadata = decoder.metadata();
    let map = symbol_map(&metadata)?;

    Ok((decoder, map))
}

pub async fn read_dbn_batch_dir(dir_path: &PathBuf) -> Result<Vec<PathBuf>> {
    // List files in directory with .zst extension
    let mut zstd_files = Vec::new();

    for entry in WalkDir::new(dir_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        if let Some(extension) = entry.path().extension() {
            if extension == "zst" {
                zstd_files.push(entry.path().to_path_buf());
            }
        }
    }

    if !zstd_files.is_empty() {
        Ok(zstd_files)
    } else {
        Err(Error::CustomError(
            "No Zstd compressed files found".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::vendors::v_databento::utils::databento_file_path;
    use databento::dbn::{Dataset, Schema};
    use time;

    fn setup(dir_path: &PathBuf) -> Result<PathBuf> {
        // Parameters
        let dataset = Dataset::GlbxMdp3;
        let start = time::macros::datetime!(2024-08-20 00:00 UTC);
        let end = time::macros::datetime!(2024-08-20 05:00 UTC);
        let schema = Schema::Mbp1;
        let symbols = vec!["ZM.n.0".to_string(), "GC.n.0".to_string()];

        // Construct file path
        let file_path = databento_file_path(dir_path, &dataset, &schema, &start, &end, &symbols)?;

        Ok(file_path)
    }

    #[tokio::test]
    #[ignore]
    async fn test_read_dbn_stream_file() -> Result<()> {
        let file_path = setup(&PathBuf::from("tests/data/databento"))?;

        // Test
        let (mut decoder, map) = read_dbn_file(file_path).await?;

        let mut records = Vec::new();
        while let Some(record) = decoder.decode_record::<dbn::Mbp1Msg>().await? {
            records.push(record.clone());
        }

        // Validate
        assert!(records.len() > 0);
        assert!(!map.is_empty(), "The map should not be empty");

        Ok(())
    }

    #[tokio::test]
    async fn test_read_dbn_batch_file() -> Result<()> {
        let dir_path = setup(&PathBuf::from("tests/data/databento/batch"))?;

        // Test
        let files = read_dbn_batch_dir(&dir_path).await?;
        for file in files {
            let (mut decoder, map) = read_dbn_file(file).await?;

            let mut records = Vec::new();
            while let Some(record) = decoder.decode_record::<dbn::Mbp1Msg>().await? {
                records.push(record.clone());
            }

            // Validate
            assert!(records.len() > 0);
            assert!(!map.is_empty(), "The map should not be empty");
        }

        Ok(())
    }
}
