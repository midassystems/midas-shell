use crate::error::Result;
use mbinary::decode::AsyncDecoder;
use mbinary::metadata::Metadata;
use mbinary::{
    self,
    encode::{MetadataEncoder, RecordEncoder},
    record_ref::RecordRef,
    records::Mbp1Msg,
};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::BufReader;

pub fn metadata_to_file(metadata: &Metadata, file_name: &PathBuf, append: bool) -> Result<()> {
    // Encode metadata
    let mut buffer = Vec::new();
    let mut encoder = MetadataEncoder::new(&mut buffer);
    encoder.encode_metadata(&metadata)?;

    let _ = encoder.write_to_file(file_name, append);

    Ok(())
}

pub async fn mbinary_to_file(
    records: &Vec<Mbp1Msg>,
    file_name: &PathBuf,
    append: bool,
) -> Result<()> {
    // Create RecordRef vector.
    let mut refs = Vec::new();
    for msg in records {
        refs.push(RecordRef::from(msg));
    }

    // Enocde records.
    let mut buffer = Vec::new();
    let mut encoder = RecordEncoder::new(&mut buffer);
    encoder.encode_records(&refs)?;

    let _ = encoder.write_to_file(file_name, append)?;

    Ok(())
}

pub async fn read_mbinary_file(filepath: &PathBuf) -> Result<AsyncDecoder<BufReader<File>>> {
    let decoder = AsyncDecoder::<BufReader<File>>::from_file(filepath).await?;

    Ok(decoder)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::vendors::databento::{
        extract::read_dbn_file,
        transform::{instrument_id_map, to_mbinary},
    };
    use mbinary::{
        self,
        record_enum::RecordEnum,
        records::{BidAskPair, Mbp1Msg, RecordHeader},
    };
    use mbinary::{
        enums::{Dataset, Schema},
        symbols::SymbolMap,
    };
    use serial_test::serial;

    async fn dummy_file() -> Result<PathBuf> {
        // Load DBN file
        let file_path = PathBuf::from(
            "tests/data/databento/GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
        );

        let (mut decoder, map) = read_dbn_file(file_path).await?;

        // MBN instrument map
        let mut mbinary_map = HashMap::new();
        mbinary_map.insert("ZM.n.0".to_string(), 20 as u32);
        mbinary_map.insert("GC.n.0".to_string(), 21 as u32);

        // Map DBN instrument to MBN insturment
        let new_map = instrument_id_map(map, mbinary_map)?;
        let metadata = Metadata::new(Schema::Mbp1, Dataset::Futures, 0, 0, SymbolMap::new());

        // Test
        let mbinary_file_name =
            PathBuf::from("tests/data/load_ZM.n.0_GC.n.0_mbp-1_2024-08-20_2024-08-20.bin");

        let _ = to_mbinary(&metadata, &mut decoder, &new_map, &mbinary_file_name).await?;

        Ok(mbinary_file_name)
    }

    #[tokio::test]
    // #[ignore]
    #[serial_test::serial]
    async fn test_read_mbinary_file() -> Result<()> {
        let file_path = dummy_file().await?;

        // let file_path = PathBuf::from("tests/data/ZM.n.0_GC.n.0_mbp-1_2024-08-20_2024-08-20.bin");

        // Test
        let mut decoder = read_mbinary_file(&file_path).await?;

        // Validate
        let mbinary_records = decoder.decode().await?;
        assert!(mbinary_records.len() > 0);

        //Cleanup
        if file_path.exists() {
            std::fs::remove_file(&file_path).expect("Failed to delete the test file.");
        }

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_mbinary_to_file() -> Result<()> {
        // Note if a fils is addee teh length woill ahve to recalculated
        let records = vec![
            Mbp1Msg {
                hd: RecordHeader::new::<Mbp1Msg>(
                    // length: 22,
                    // rtype: 1,
                    1333,
                    1724079906415347717,
                    0,
                ),
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
                hd: RecordHeader::new::<Mbp1Msg>(
                    // length: 22,
                    // rtype: 1,
                    1333,
                    1724079906415347717,
                    0,
                ),
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
                hd: RecordHeader::new::<Mbp1Msg>(
                    // length: 22,
                    // rtype: 1,
                    1333,
                    1724079906415347717,
                    0,
                ),
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
                hd: RecordHeader::new::<Mbp1Msg>(
                    // length: 22,
                    // rtype: 1,
                    1333,
                    1724079906415347717,
                    0,
                ),
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
                hd: RecordHeader::new::<Mbp1Msg>(
                    // length: 22,
                    // rtype: 1,
                    1333,
                    1724079906415347717,
                    0,
                ),
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

        // Test
        let path = PathBuf::from("tests/data/test_mbinary_to_file.bin");
        mbinary_to_file(&records, &path, false).await?;

        // Validate
        let mut buffer = Vec::new();
        let mut decoder = AsyncDecoder::<BufReader<File>>::from_file(path.clone()).await?;
        while let Some(record_ref) = decoder.decode_ref().await? {
            let rec_enum = RecordEnum::from_ref(record_ref);
            buffer.push(rec_enum?);
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
