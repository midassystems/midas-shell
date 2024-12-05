use crate::error::Result;
use mbn::decode::AsyncDecoder;
use mbn::record_enum::RecordEnum;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::BufReader;

pub async fn find_duplicates(filepath: &PathBuf) -> Result<usize> {
    let mut occurrences: HashMap<RecordEnum, usize> = HashMap::new();
    let mut decoder = AsyncDecoder::<BufReader<File>>::from_file(filepath).await?;

    // Decode and count occurrences of each record
    while let Some(record_ref) = decoder.decode_ref().await? {
        let record_enum = RecordEnum::from_ref(record_ref)?;
        *occurrences.entry(record_enum).or_default() += 1;
    }

    // Identify duplicates
    let duplicates: Vec<_> = occurrences
        .iter()
        .filter(|&(_, &count)| count > 1)
        .collect();

    // Respond based on results
    if duplicates.is_empty() {
        println!("No duplicate records found in the file.");
    } else {
        println!("Found {} duplicate records in the file:", duplicates.len());
        for (record, count) in &duplicates {
            println!("{:?} - {} occurrences", record, count);
        }
    }

    Ok(duplicates.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use mbn::encode::RecordEncoder;
    use mbn::record_ref::RecordRef;
    use mbn::{
        self,
        records::{BidAskPair, Mbp1Msg, RecordHeader},
    };
    use serial_test::serial;
    use std::path::PathBuf;

    #[tokio::test]
    #[serial]
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

        let mut record_refs: Vec<RecordRef> = Vec::new();

        for record in &records {
            record_refs.push(RecordRef::from(record));
        }

        let mut buffer = Vec::new();
        let mut encoder = RecordEncoder::new(&mut buffer);
        encoder
            .encode_records(&record_refs)
            .expect("Encoding failed");

        let file = "tests/data/test_duplicates_output.bin";
        let path = PathBuf::from(file);
        let _ = encoder.write_to_file(&path);

        // Test
        let num_duplicates = find_duplicates(&path).await?;

        // Validate
        assert!(num_duplicates == 1);

        // Cleanup
        if path.exists() {
            std::fs::remove_file(&path).expect("Failed to delete the test file.");
        }

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_find_duplicate_false() -> Result<()> {
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
                price: 77025000000,
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
                price: 78025000000,
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
                price: 79050000000,
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

        let mut record_refs: Vec<RecordRef> = Vec::new();

        for record in &records {
            record_refs.push(RecordRef::from(record));
        }

        let mut buffer = Vec::new();
        let mut encoder = RecordEncoder::new(&mut buffer);
        encoder
            .encode_records(&record_refs)
            .expect("Encoding failed");

        let file = "tests/data/test_duplicates_output.bin";
        let path = PathBuf::from(file);
        let _ = encoder.write_to_file(&path);

        // Test
        // let file_path = PathBuf::from("tests/data/midas/mbp1_test.bin");
        let num_duplicates = find_duplicates(&path).await?;

        // Validate
        assert!(num_duplicates == 0);

        // Cleanup
        if path.exists() {
            std::fs::remove_file(&path).expect("Failed to delete the test file.");
        }

        Ok(())
    }
}
