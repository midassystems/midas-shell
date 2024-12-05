use super::super::super::midas::load::mbn_to_file;
use crate::error;
use crate::error::{Error, Result};
use async_compression::tokio::bufread::ZstdDecoder;
use databento::{dbn, historical::timeseries::AsyncDbnDecoder};
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

fn iterate_flag(block: &Vec<Mbp1Msg>, msg: &mut Mbp1Msg) -> Mbp1Msg {
    if block.iter().any(|m| m == msg) {
        // Duplicate found in the block
        msg.flags += 1;
        iterate_flag(block, msg)
    } else {
        msg.clone()
    }
}

pub async fn to_mbn(
    decoder: &mut AsyncDbnDecoder<ZstdDecoder<BufReader<File>>>,
    new_map: &HashMap<u32, u32>,
    file_name: &PathBuf,
) -> Result<()> {
    let mut mbn_records = Vec::new();
    let mut rolling_block: Vec<Mbp1Msg> = Vec::new();
    let batch_size = 10000;

    // Decode each record and process it on the fly
    while let Some(record) = decoder.decode_record::<dbn::Mbp1Msg>().await? {
        let mut mbn_msg = Mbp1Msg::from(record);

        if let Some(new_id) = new_map.get(&mbn_msg.hd.instrument_id) {
            mbn_msg.hd.instrument_id = *new_id;
        }

        if mbn_msg.flags == 0 {
            let updated_msg = iterate_flag(&rolling_block, &mut mbn_msg);
            rolling_block.push(updated_msg);
        } else {
            rolling_block.clear();
        }

        mbn_records.push(mbn_msg);

        // If batch is full, write to file and clear batch
        if mbn_records.len() >= batch_size {
            mbn_to_file(&mbn_records, file_name).await?;
            mbn_records.clear();
        }
    }

    // Write any remaining records in the last batch
    if !mbn_records.is_empty() {
        mbn_to_file(&mbn_records, file_name).await?;
        mbn_records.clear();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::pipeline::vendors::v_databento::{
        extract::read_dbn_file, utils::databento_file_name,
    };
    use databento::dbn::{Dataset, Schema};
    use std::fs;
    use std::path::PathBuf;
    use time;

    fn setup(dir_path: &PathBuf, batch: bool) -> Result<PathBuf> {
        // Parameters
        let dataset = Dataset::GlbxMdp3;
        let start = time::macros::datetime!(2024-08-20 00:00 UTC);
        let end = time::macros::datetime!(2024-08-20 05:00 UTC);
        let schema = Schema::Mbp1;
        let symbols = vec!["ZM.n.0".to_string(), "GC.n.0".to_string()];

        // Construct file path
        let file_path = databento_file_name(&dataset, &schema, &start, &end, &symbols, batch)?;
        Ok(dir_path.join(file_path))
    }

    #[tokio::test]
    async fn test_instrument_id_map() -> Result<()> {
        // Load DBN file
        let file_path = setup(&PathBuf::from("tests/data/databento"), false)?;

        let (_decoder, map) = read_dbn_file(file_path).await?;

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
    async fn test_instrument_id_map_error() -> Result<()> {
        // Load DBN file
        let file_path = setup(&PathBuf::from("tests/data/databento"), false)?;

        let (_decoder, map) = read_dbn_file(file_path).await?;

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
    async fn test_to_mbn() -> Result<()> {
        // Load DBN file
        let file_path = setup(&PathBuf::from("tests/data/databento"), false)?;

        let (mut decoder, map) = read_dbn_file(file_path).await?;

        // MBN instrument map
        let mut mbn_map = HashMap::new();
        mbn_map.insert("ZM.n.0".to_string(), 20 as u32);
        mbn_map.insert("GC.n.0".to_string(), 20 as u32);

        // Map DBN instrument to MBN insturment
        let new_map = instrument_id_map(map, mbn_map)?;

        // Test
        let start = time::macros::datetime!(2024-08-20 00:00 UTC);
        let end = time::macros::datetime!(2024-08-20 05:00 UTC);

        let mbn_file_name = PathBuf::from(format!(
            "tests/data/databento/{}_{}_{}_{}.bin",
            "ZM.n.0_GC.n.0",
            "mbp-1",
            start.date(),
            end.date(),
        ));

        let _ = to_mbn(&mut decoder, &new_map, &mbn_file_name).await?;

        // Validate
        assert!(fs::metadata(&mbn_file_name).is_ok(), "File does not exist");

        Ok(())
    }
}
