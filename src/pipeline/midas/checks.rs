use crate::error::Result;
use crate::pipeline::midas::load::read_mbinary_file;
use crate::pipeline::vendors::v_databento::extract::read_dbn_file;
use mbinary::decode::AsyncDecoder;
use mbinary::record_enum::RecordEnum;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;

pub async fn compare_dbn_raw_output(
    dbn_filepath: PathBuf,
    mbinary_filepath: &PathBuf,
) -> Result<()> {
    let mut mbinary_decoder = read_mbinary_file(mbinary_filepath).await?;
    let (mut dbn_decoder, _map) = read_dbn_file(dbn_filepath).await?;
    println!("{:?}", dbn_decoder.metadata());
    println!("{:?}", mbinary_decoder.metadata());

    // Output files
    let mbinary_output_file = "raw_mbinary_records.txt";
    let dbn_output_file = "raw_dbn_records.txt";

    // Create or truncate output files
    let mut mbinary_file = File::create(mbinary_output_file).await?;
    let mut dbn_file = File::create(dbn_output_file).await?;

    let mut mbinary_count = 0;
    // Write MBN records to file
    while let Some(mbinary_record) = mbinary_decoder.decode_ref().await? {
        mbinary_count += 1;
        let record_enum = RecordEnum::from_ref(mbinary_record)?;
        mbinary_file
            .write_all(format!("{:?}\n", record_enum).as_bytes())
            .await?;
    }

    let mut dbn_count = 0;
    // Write DBN records to file
    while let Some(dbn_record) = dbn_decoder.decode_record_ref().await? {
        dbn_count += 1;
        let dbn_record_enum = dbn_record.as_enum()?.to_owned();
        dbn_file
            .write_all(format!("{:?}\n", dbn_record_enum).as_bytes())
            .await?;
    }
    println!("MBN length: {:?}", mbinary_count);
    println!("DBN length: {:?}", dbn_count);

    println!(
        "MBN records written to: {}, DBN records written to: {}",
        mbinary_output_file, dbn_output_file
    );

    Ok(())
}

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
    use crate::pipeline::vendors::v_databento::transform::{instrument_id_map, to_mbinary};
    use mbinary::encode::RecordEncoder;
    use mbinary::enums::{Dataset, Schema};
    use mbinary::metadata::Metadata;
    use mbinary::record_ref::RecordRef;
    use mbinary::symbols::SymbolMap;
    use mbinary::{
        self,
        records::{BidAskPair, Mbp1Msg, RecordHeader},
    };
    use serial_test::serial;
    use std::path::PathBuf;

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_find_duplicate_error_fix() -> Result<()> {
        let mbinary_file = PathBuf::from("tests/data/HEQ4_mbp1.bin");
        let dbn_file = PathBuf::from(
            "tests/data/databento/GLBX.MDP3_mbp-1_HEQ4_2024-03-05T00:00:00Z_2024-03-06T00:00:00Z.dbn",
        );

        // Convert to mbinary
        let (mut decoder, map) = read_dbn_file(dbn_file.clone()).await?;

        let mut mbinary_map = HashMap::new();
        mbinary_map.insert("HEQ4".to_string(), 21 as u32);
        let new_map = instrument_id_map(map, mbinary_map)?;
        let metadata = Metadata::new(Schema::Mbp1, Dataset::Futures, 0, 0, SymbolMap::new());

        let _ = to_mbinary(&metadata, &mut decoder, &new_map, &mbinary_file).await?;

        // Compare files
        compare_dbn_raw_output(dbn_file.clone(), &mbinary_file).await?;

        // Check duplicates mbinary
        let duplicates_count = find_duplicates(&mbinary_file).await?;
        assert_eq!(duplicates_count, 0);

        // Cleanup
        if mbinary_file.exists() {
            std::fs::remove_file(&mbinary_file).expect("Failed to delete the test file.");
        }
        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_find_duplicate_true() -> Result<()> {
        let records = vec![
            Mbp1Msg {
                hd: RecordHeader::new::<Mbp1Msg>(1333, 1724079906415347717, 0),
                price: 76025000000,
                size: 2,
                action: 84,
                side: 66,
                depth: 0,
                flags: 1,
                ts_recv: 1724079906416004321,
                ts_in_delta: 17171,
                sequence: 900097,
                discriminator: 0,
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
                hd: RecordHeader::new::<Mbp1Msg>(1333, 1724079906415347717, 0),
                price: 76025000000,
                size: 2,
                action: 84,
                side: 66,
                depth: 0,
                flags: 1,
                ts_recv: 1724079906416004321,
                ts_in_delta: 17171,
                sequence: 900097,
                discriminator: 0,
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
                hd: RecordHeader::new::<Mbp1Msg>(1333, 1724079906415347717, 0),
                price: 76025000000,
                size: 2,
                action: 84,
                side: 66,
                depth: 0,
                flags: 1,
                ts_recv: 1724079906416004321,
                ts_in_delta: 17171,
                sequence: 900097,
                discriminator: 0,
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
                hd: RecordHeader::new::<Mbp1Msg>(1333, 1724079906415347717, 0),
                price: 76025000000,
                size: 2,
                action: 84,
                side: 66,
                depth: 0,
                flags: 0,
                ts_recv: 1724079906416004321,
                ts_in_delta: 17171,
                sequence: 900097,
                discriminator: 0,
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
                hd: RecordHeader::new::<Mbp1Msg>(1333, 1724079906415347717, 0),
                price: 76050000000,
                size: 1,
                action: 84,
                side: 66,
                depth: 0,
                flags: 0,
                ts_recv: 1724079906416018707,
                ts_in_delta: 13985,
                sequence: 900098,
                discriminator: 0,
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
        let _ = encoder.write_to_file(&path, false);

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
                hd: RecordHeader::new::<Mbp1Msg>(1333, 1724079906415347717, 0),
                price: 76025000000,
                size: 2,
                action: 84,
                side: 66,
                depth: 0,
                flags: 1,
                ts_recv: 1724079906416004321,
                ts_in_delta: 17171,
                sequence: 900097,
                discriminator: 0,
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
                hd: RecordHeader::new::<Mbp1Msg>(1333, 1724079906415347717, 0),
                price: 77025000000,
                size: 1,
                action: 84,
                side: 66,
                depth: 0,
                flags: 0,
                ts_recv: 1724079906416018707,
                ts_in_delta: 13985,
                sequence: 900098,
                discriminator: 0,
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
                hd: RecordHeader::new::<Mbp1Msg>(1333, 1724079906415347717, 0),
                price: 78025000000,
                size: 2,
                action: 84,
                side: 66,
                depth: 0,
                flags: 0,
                ts_recv: 1724079906416004321,
                ts_in_delta: 17171,
                sequence: 900097,
                discriminator: 0,
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
                hd: RecordHeader::new::<Mbp1Msg>(1333, 1724079906415347717, 0),
                price: 79050000000,
                size: 1,
                action: 84,
                side: 66,
                depth: 0,
                flags: 0,
                ts_recv: 1724079906416018707,
                ts_in_delta: 13985,
                sequence: 900098,
                discriminator: 0,
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
        let _ = encoder.write_to_file(&path, false);

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
