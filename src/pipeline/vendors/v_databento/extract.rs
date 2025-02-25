use crate::error::{Error, Result};
use async_compression::tokio::bufread::ZstdDecoder;
use databento::historical::timeseries::AsyncDbnDecoder;
use dbn;
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
        .map_err(|_| Error::CustomError("Error opening dbn file.".to_string()))?;

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

    #[tokio::test]
    #[serial_test::serial]
    // #[ignore]
    async fn test_read_dbn_file() -> Result<()> {
        let file_path = PathBuf::from(
            "tests/data/databento/GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
        );

        // Test
        let (mut decoder, _) = read_dbn_file(file_path).await?;

        // Validate
        let _metadata = decoder.metadata();

        // Decode to vector of messages
        let mut dbn_records = Vec::new();
        while let Some(record) = decoder.decode_record_ref().await? {
            let record_enum = record.as_enum()?;
            dbn_records.push(record_enum.to_owned());
        }

        assert!(dbn_records.len() > 0);

        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_read_dbn_stream_file() -> Result<()> {
        let dir_path = PathBuf::from("tests/data/databento/batch_GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn");

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
