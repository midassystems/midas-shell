use crate::error::Result;
use mbn::decode::AsyncDecoder;
use mbn::{self, encode::RecordEncoder, record_ref::RecordRef, records::Mbp1Msg};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::BufReader;

pub async fn mbn_to_file(records: &Vec<Mbp1Msg>, file_name: &PathBuf) -> Result<()> {
    // Create RecordRef vector.
    let mut refs = Vec::new();
    for msg in records {
        refs.push(RecordRef::from(msg));
    }

    // Enocde records.
    let mut buffer = Vec::new();
    let mut encoder = RecordEncoder::new(&mut buffer);
    encoder.encode_records(&refs)?;

    let _ = encoder.write_to_file(file_name)?;

    Ok(())
}

pub async fn read_mbn_file(filepath: &PathBuf) -> Result<AsyncDecoder<BufReader<File>>> {
    let decoder = AsyncDecoder::<BufReader<File>>::from_file(filepath).await?;

    Ok(decoder)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::date_to_unix_nanos;
    use dotenv::dotenv;
    use mbn::symbols::Instrument;
    use mbn::{
        self,
        record_enum::RecordEnum,
        records::{BidAskPair, Mbp1Msg, RecordHeader},
    };
    use serial_test::serial;

    // -- Helper --
    async fn create_test_ticker(ticker: &str, name: &str) -> Result<()> {
        dotenv().ok();
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = midas_client::historical::Historical::new(base_url);

        let first_available = date_to_unix_nanos("2024-08-20")?;
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            mbn::symbols::Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            first_available as u64,
            first_available as u64,
            true,
        );

        client.create_symbol(&instrument).await?;

        Ok(())
    }

    async fn cleanup_test_ticker(ticker: &str) -> Result<()> {
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = midas_client::historical::Historical::new(base_url);
        let id = client.get_symbol(&ticker.to_string()).await?.data;
        // .expect("Error getting test ticker from server.");

        let _ = client.delete_symbol(&(id as i32)).await?;

        Ok(())
    }

    #[tokio::test]
    // #[ignore]
    async fn test_read_mbn_file() -> Result<()> {
        let file_path = PathBuf::from(
            "tests/data/ZM.n.0_GC.n.0_mbp-1_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.bin",
        );

        // Test
        let mut decoder = read_mbn_file(&file_path).await?;

        // Validate
        let mbn_records = decoder.decode().await?;
        assert!(mbn_records.len() > 0);

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_mbn_to_file() -> Result<()> {
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

        // Test
        let path = PathBuf::from("tests/data/test_mbn_to_file.bin");
        mbn_to_file(&records, &path).await?;

        // Validate
        let mut buffer = Vec::new();
        let mut decoder = AsyncDecoder::<BufReader<File>>::from_file(path.clone()).await?;
        while let Some(record_ref) = decoder.decode_ref().await? {
            buffer.push(RecordEnum::from_ref(record_ref)?);
        }

        // Validate
        assert!(buffer.len() > 0);

        // Cleanup
        if path.exists() {
            std::fs::remove_file(&path).expect("Failed to delete the test file.");
        }

        Ok(())
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::error::Result;
//     use crate::pipeline::vendors::v_databento::{
//         extract::read_dbn_file, utils::databento_file_name,
//     };
//     use databento::dbn::{Dataset, Schema};
//     use mbn::{
//         self,
//         records::{BidAskPair, RecordHeader},
//     };
//     use std::fs;
//     use std::path::PathBuf;
//     use time;
//     fn setup(dir_path: &PathBuf, batch: bool) -> Result<PathBuf> {
//         // Parameters
//         let dataset = Dataset::GlbxMdp3;
//         let start = time::macros::datetime!(2024-08-20 00:00 UTC);
//         let end = time::macros::datetime!(2024-08-20 05:00 UTC);
//         let schema = Schema::Mbp1;
//         let symbols = vec!["ZM.n.0".to_string(), "GC.n.0".to_string()];
//
//         // Construct file path
//         let file_path = databento_file_name(&dataset, &schema, &start, &end, &symbols, batch)?;
//         Ok(dir_path.join(file_path))
//     }
//
//     #[tokio::test]
//     async fn test_mbn_to_file() -> Result<()> {
//         // Load DBN file
//         let file_path = setup(&PathBuf::from("tests/data/databento"), false)?;
//
//         let (mut decoder, map) = read_dbn_file(file_path).await?;
//
//         // MBN instrument map
//         let mut mbn_map = HashMap::new();
//         mbn_map.insert("ZM.n.0".to_string(), 20 as u32);
//         mbn_map.insert("GC.n.0".to_string(), 20 as u32);
//
//         // Map DBN instrument to MBN insturment
//         let new_map = instrument_id_map(map, mbn_map)?;
//
//         // Test
//         let start = time::macros::datetime!(2024-08-20 00:00 UTC);
//         let end = time::macros::datetime!(2024-08-20 05:00 UTC);
//
//         let mbn_file_name = PathBuf::from(format!(
//             "tests/data/databento/{}_{}_{}_{}.bin",
//             "ZM.n.0_GC.n.0",
//             "mbp-1",
//             start.date(),
//             end.date(),
//         ));
//
//         let _ = to_mbn(&mut decoder, &new_map, &mbn_file_name).await?;
//
//         // Validate
//         assert!(fs::metadata(&mbn_file_name).is_ok(), "File does not exist");
//
//         Ok(())
//     }
// }
