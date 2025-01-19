use super::super::super::midas::load::mbn_to_file;
use crate::error;
use crate::error::{Error, Result};
use crate::pipeline::midas::load::metadata_to_file;
use async_compression::tokio::bufread::ZstdDecoder;
use databento::{dbn, historical::timeseries::AsyncDbnDecoder};
use mbn::metadata::Metadata;
use mbn::{self, records::Mbp1Msg};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::BufReader;

pub fn instrument_id_map(
    dbn_map: HashMap<String, String>,
    mbn_map: HashMap<String, u32>,
) -> Result<HashMap<u32, u32>> {
    let mut map = HashMap::new();

    for (id, ticker) in dbn_map.iter() {
        if let Some(mbn_id) = mbn_map.get(ticker) {
            if let Ok(parsed_id) = id.parse::<u32>() {
                map.insert(parsed_id, *mbn_id);
            } else {
                return Err(error!(CustomError, "Failed to parse id: {}", id));
            }
        } else {
            return Err(error!(CustomError, "Ticker {} not in database.", ticker));
        }
    }
    Ok(map)
}

pub async fn to_mbn(
    metadata: &Metadata,
    decoder: &mut AsyncDbnDecoder<ZstdDecoder<BufReader<File>>>,
    map: &HashMap<u32, u32>,
    file_name: &PathBuf,
) -> Result<()> {
    let mut mbn_records = Vec::new();
    let mut block: HashMap<Mbp1Msg, u32> = HashMap::new();
    let batch_size = 10000;

    let _ = metadata_to_file(&metadata, file_name, true)?;

    // Decode each record and process it on the fly
    while let Some(record) = decoder.decode_record::<dbn::Mbp1Msg>().await? {
        let mut mbn_msg = Mbp1Msg::from(record);

        if let Some(new_id) = map.get(&mbn_msg.hd.instrument_id) {
            mbn_msg.hd.instrument_id = *new_id;
        }

        if record.flags.is_last() {
            block.clear();
        }

        block
            .entry(mbn_msg.clone())
            .and_modify(|v| *v += 1)
            .or_insert(0);

        if let Some(count) = block.get(&mbn_msg) {
            mbn_msg.discriminator = *count;
        }

        mbn_records.push(mbn_msg);

        // If batch is full, write to file and clear batch
        if mbn_records.len() >= batch_size {
            mbn_to_file(&mbn_records, file_name, true).await?;
            mbn_records.clear();
        }
    }

    // Write any remaining records in the last batch
    if !mbn_records.is_empty() {
        mbn_to_file(&mbn_records, file_name, true).await?;
        mbn_records.clear();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::pipeline::vendors::v_databento::extract::read_dbn_file;
    use mbn::{
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
        let mut mbn_map = HashMap::new();
        mbn_map.insert("ZM.n.0".to_string(), 20 as u32);
        mbn_map.insert("GC.n.0".to_string(), 20 as u32);

        // Test
        let response = instrument_id_map(map, mbn_map)?;

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
        let mut mbn_map = HashMap::new();
        mbn_map.insert("ZM.n.0".to_string(), 20 as u32);

        // Test
        let response = instrument_id_map(map, mbn_map);

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
    async fn test_to_mbn() -> Result<()> {
        // Load DBN file
        let file_path = PathBuf::from(
            "tests/data/databento/GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
        );

        let (mut decoder, map) = read_dbn_file(file_path.clone()).await?;

        // MBN instrument map
        let mut mbn_map = HashMap::new();
        mbn_map.insert("ZM.n.0".to_string(), 20 as u32);
        mbn_map.insert("GC.n.0".to_string(), 21 as u32);

        // Map DBN instrument to MBN insturment
        let new_map = instrument_id_map(map, mbn_map)?;

        // Test
        let start = time::macros::datetime!(2024-08-20 00:00 UTC);
        let end = time::macros::datetime!(2024-08-20 05:00 UTC);

        let mbn_file_name = PathBuf::from(format!(
            "tests/data/{}_{}_{}_{}.bin",
            "ZM.n.0_GC.n.0",
            "mbp-1",
            start.date(),
            end.date(),
        ));

        let metadata = Metadata::new(Schema::Mbp1, Dataset::Futures, 0, 0, SymbolMap::new());

        let _ = to_mbn(&metadata, &mut decoder, &new_map, &mbn_file_name).await?;

        // Validate
        assert!(fs::metadata(&mbn_file_name).is_ok(), "File does not exist");

        //Cleanup
        let mbn_path = PathBuf::from("tests/data/ZM.n.0_GC.n.0_mbp-1_2024-08-20_2024-08-20.bin");

        if mbn_path.exists() {
            std::fs::remove_file(&mbn_path).expect("Failed to delete the test file.");
        }
        Ok(())
    }
}
