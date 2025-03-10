use crate::error;
use crate::error::{Error, Result};
use crate::vendors::midas::load::mbinary_to_file;
use crate::vendors::midas::load::metadata_to_file;
use async_compression::tokio::bufread::ZstdDecoder;
use databento::historical::timeseries::AsyncDbnDecoder;
use dbn;
use mbinary::metadata::Metadata;
use mbinary::{self, records::Mbp1Msg};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::BufReader;

pub fn instrument_id_map(
    dbn_map: HashMap<String, String>,
    mbinary_map: HashMap<String, u32>,
) -> Result<HashMap<u32, u32>> {
    let mut map = HashMap::new();

    for (id, ticker) in dbn_map.iter() {
        if let Some(mbinary_id) = mbinary_map.get(ticker) {
            if let Ok(parsed_id) = id.parse::<u32>() {
                map.insert(parsed_id, *mbinary_id);
            } else {
                return Err(error!(CustomError, "Failed to parse id: {}", id));
            }
        } else {
            return Err(error!(CustomError, "Ticker {} not in database.", ticker));
        }
    }
    Ok(map)
}

pub async fn to_mbinary(
    metadata: &Metadata,
    decoder: &mut AsyncDbnDecoder<ZstdDecoder<BufReader<File>>>,
    map: &HashMap<u32, u32>,
    file_name: &PathBuf,
) -> Result<()> {
    let mut mbinary_records = Vec::new();
    let mut block: HashMap<u64, HashMap<Mbp1Msg, u32>> = HashMap::new();
    let batch_size: usize = 10000;

    let _ = metadata_to_file(&metadata, file_name, true)?;

    // Decode each record and process it on the fly
    while let Some(record) = decoder.decode_record::<dbn::Mbp1Msg>().await? {
        let mut mbinary_msg = Mbp1Msg::from(record);

        if let Some(new_id) = map.get(&mbinary_msg.hd.instrument_id) {
            mbinary_msg.hd.instrument_id = *new_id;
        }

        // Prune old records from the block
        let ts_recv = mbinary_msg.ts_recv;
        block.retain(|key, _| *key >= ts_recv);

        // Insert or update the current record in the block
        block
            .entry(ts_recv) // Outer key is ts_recv
            .or_default() // Ensure the inner map exists
            .entry(mbinary_msg.clone()) // Inner key is Mbp1Msg
            .and_modify(|v| *v += 1)
            .or_insert(0);

        // Update the discriminator based on the count in the block
        if let Some(count) = block
            .get(&ts_recv)
            .and_then(|inner| inner.get(&mbinary_msg))
        {
            mbinary_msg.discriminator = *count;
        }

        mbinary_records.push(mbinary_msg);

        // If batch is full, write to file and clear batch
        if mbinary_records.len() >= batch_size {
            mbinary_to_file(&mbinary_records, file_name, true).await?;
            mbinary_records.clear();
        }
    }

    // Write any remaining records in the last batch
    if !mbinary_records.is_empty() {
        mbinary_to_file(&mbinary_records, file_name, true).await?;
        mbinary_records.clear();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::vendors::databento::extract::read_dbn_file;
    use mbinary::{
        enums::{Dataset, Schema},
        symbols::SymbolMap,
    };
    use std::fs;
    use std::path::PathBuf;
    use time;

    #[tokio::test]
    #[serial_test::serial]
    async fn test_instrument_id_map() -> Result<()> {
        // Load DBN file
        let file_path = PathBuf::from(
            "tests/data/databento/GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
        );

        let (_decoder, map) = read_dbn_file(file_path.clone()).await?;

        // MBN instrument map
        let mut mbinary_map = HashMap::new();
        mbinary_map.insert("ZM.n.0".to_string(), 20 as u32);
        mbinary_map.insert("GC.n.0".to_string(), 20 as u32);

        // Test
        let response = instrument_id_map(map, mbinary_map)?;

        // Validate
        let mut expected_map = HashMap::new();
        expected_map.insert(377503, 20);
        expected_map.insert(393, 20);
        assert_eq!(expected_map, response);

        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_instrument_id_map_error() -> Result<()> {
        // Load DBN file
        let file_path = PathBuf::from(
            "tests/data/databento/GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
        );

        let (_decoder, map) = read_dbn_file(file_path.clone()).await?;

        // MBN instrument map
        let mut mbinary_map = HashMap::new();
        mbinary_map.insert("ZM.n.0".to_string(), 20 as u32);

        // Test
        let response = instrument_id_map(map, mbinary_map);

        // Validate
        assert!(
            matches!(response, Err(_)),
            "Expected an error, but got: {:?}",
            response
        );

        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_to_mbinary() -> Result<()> {
        // Load DBN file
        let file_path = PathBuf::from(
            "tests/data/databento/GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
        );

        let (mut decoder, map) = read_dbn_file(file_path.clone()).await?;

        // MBN instrument map
        let mut mbinary_map = HashMap::new();
        mbinary_map.insert("ZM.n.0".to_string(), 20 as u32);
        mbinary_map.insert("GC.n.0".to_string(), 21 as u32);

        // Map DBN instrument to MBN insturment
        let new_map = instrument_id_map(map, mbinary_map)?;

        // Test
        let start = time::macros::datetime!(2024-08-20 00:00 UTC);
        let end = time::macros::datetime!(2024-08-20 05:00 UTC);

        let mbinary_file_name = PathBuf::from(format!(
            "tests/data/{}_{}_{}_{}.bin",
            "ZM.n.0_GC.n.0",
            "mbp-1",
            start.date(),
            end.date(),
        ));

        let metadata = Metadata::new(Schema::Mbp1, Dataset::Futures, 0, 0, SymbolMap::new());

        let _ = to_mbinary(&metadata, &mut decoder, &new_map, &mbinary_file_name).await?;

        // Validate
        assert!(
            fs::metadata(&mbinary_file_name).is_ok(),
            "File does not exist"
        );

        //Cleanup
        let mbinary_path =
            PathBuf::from("tests/data/ZM.n.0_GC.n.0_mbp-1_2024-08-20_2024-08-20.bin");

        if mbinary_path.exists() {
            std::fs::remove_file(&mbinary_path).expect("Failed to delete the test file.");
        }
        Ok(())
    }
}
