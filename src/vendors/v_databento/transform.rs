use crate::error::{Error, Result};
use async_compression::tokio::bufread::ZstdDecoder;
use databento::{dbn, historical::timeseries::AsyncDbnDecoder};
use mbn::{self, encode::RecordEncoder, record_ref::RecordRef, records::Mbp1Msg};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::BufReader;

pub fn instrument_id_map(
    dbn_map: HashMap<String, String>,
    mbn_map: HashMap<String, u32>,
) -> Result<HashMap<u32, u32>> {
    // Create the new map
    let mut map = HashMap::new();

    for (id, ticker) in dbn_map.iter() {
        if let Some(mbn_id) = mbn_map.get(ticker) {
            if let Ok(parsed_id) = id.parse::<u32>() {
                map.insert(parsed_id, *mbn_id);
            } else {
                return Err(Error::Conversion(format!("Failed to parse id: {}", id)));
            }
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

async fn mbn_to_file(records: &Vec<Mbp1Msg>, file_name: &PathBuf) -> Result<()> {
    // Create RecordRef vector.
    let mut refs = Vec::new();
    for msg in records {
        refs.push(RecordRef::from(msg));
    }

    // Enocde records.
    let mut buffer = Vec::new();
    let mut encoder = RecordEncoder::new(&mut buffer);
    encoder.encode_records(&refs)?;

    // Output to file
    let _ = encoder.write_to_file(file_name)?;

    Ok(())
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

pub fn find_duplicates(mbps: &Vec<mbn::records::Mbp1Msg>) -> Result<usize> {
    let mut occurrences = HashMap::new();
    let mut duplicates = Vec::new();

    for msg in mbps {
        // Only consider messages with non-zero flags as potential duplicates
        let count = occurrences.entry(msg.clone()).or_insert(0);
        *count += 1;
    }

    for msg in mbps {
        // Again, consider only messages with non-zero flags
        if let Some(&count) = occurrences.get(&msg) {
            if count > 1 {
                duplicates.push(msg);
            }
        }
    }

    Ok(duplicates.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::vendors::v_databento::{extract::read_dbn_file, utils::databento_file_path};
    use databento::dbn::{Dataset, Schema};
    use mbn::{
        self,
        records::{BidAskPair, RecordHeader},
    };
    use std::fs;
    use std::path::PathBuf;
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
    async fn test_mbn_to_file() -> Result<()> {
        // Load DBN file
        let file_path = setup(&PathBuf::from("tests/data/databento"))?;

        let (mut decoder, map) = read_dbn_file(file_path).await?;

        // MBN instrument map
        let mut mbn_map = HashMap::new();
        mbn_map.insert("ZM.n.0".to_string(), 20 as u32);

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

    #[tokio::test]
    async fn test_find_duplicate_true() -> Result<()> {
        let records = vec![
            Mbp1Msg {
                hd: RecordHeader {
                    length: 20,
                    rtype: 1,
                    instrument_id: 1333,
                    ts_event: 1724079906415347717,
                },
                price: 76025000000,
                size: 2,
                action: 84,
                side: 66,
                depth: 0,
                flags: 1,
                ts_recv: 1724079906416004321,
                ts_in_delta: 17171,
                sequence: 900097,
                levels: [BidAskPair {
                    bid_px: 76000000000,
                    ask_px: 76025000000,
                    bid_sz: 7,
                    ask_sz: 3,
                    bid_ct: 6,
                    ask_ct: 3,
                }],
            },
            Mbp1Msg {
                hd: RecordHeader {
                    length: 20,
                    rtype: 1,
                    instrument_id: 1333,
                    ts_event: 1724079906415347717,
                },
                price: 76025000000,
                size: 2,
                action: 84,
                side: 66,
                depth: 0,
                flags: 1,
                ts_recv: 1724079906416004321,
                ts_in_delta: 17171,
                sequence: 900097,
                levels: [BidAskPair {
                    bid_px: 76000000000,
                    ask_px: 76025000000,
                    bid_sz: 7,
                    ask_sz: 3,
                    bid_ct: 6,
                    ask_ct: 3,
                }],
            },
            Mbp1Msg {
                hd: RecordHeader {
                    length: 20,
                    rtype: 1,
                    instrument_id: 1333,
                    ts_event: 1724079906415347717,
                },
                price: 76025000000,
                size: 1,
                action: 84,
                side: 66,
                depth: 0,
                flags: 0,
                ts_recv: 1724079906416018707,
                ts_in_delta: 13985,
                sequence: 900098,
                levels: [BidAskPair {
                    bid_px: 76000000000,
                    ask_px: 76025000000,
                    bid_sz: 7,
                    ask_sz: 3,
                    bid_ct: 6,
                    ask_ct: 3,
                }],
            },
            Mbp1Msg {
                hd: RecordHeader {
                    length: 20,
                    rtype: 1,
                    instrument_id: 1333,
                    ts_event: 1724079906415347717,
                },
                price: 76025000000,
                size: 2,
                action: 84,
                side: 66,
                depth: 0,
                flags: 0,
                ts_recv: 1724079906416004321,
                ts_in_delta: 17171,
                sequence: 900097,
                levels: [BidAskPair {
                    bid_px: 76000000000,
                    ask_px: 76025000000,
                    bid_sz: 7,
                    ask_sz: 3,
                    bid_ct: 6,
                    ask_ct: 3,
                }],
            },
            Mbp1Msg {
                hd: RecordHeader {
                    length: 20,
                    rtype: 1,
                    instrument_id: 1333,
                    ts_event: 1724079906415347717,
                },
                price: 76050000000,
                size: 1,
                action: 84,
                side: 66,
                depth: 0,
                flags: 0,
                ts_recv: 1724079906416018707,
                ts_in_delta: 13985,
                sequence: 900098,
                levels: [BidAskPair {
                    bid_px: 76000000000,
                    ask_px: 76025000000,
                    bid_sz: 7,
                    ask_sz: 3,
                    bid_ct: 6,
                    ask_ct: 3,
                }],
            },
        ];
        // Test
        let num_duplicates = find_duplicates(&records)?;

        // Validate
        assert!(num_duplicates > 0);

        Ok(())
    }
}