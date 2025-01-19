use crate::error::Result;
use mbn::decode::AsyncDecoder;
use mbn::record_enum::RecordEnum;
use mbn::records::Record;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;

pub async fn compare_mbn(mbn_filepath1: &PathBuf, mbn_filepath2: &PathBuf) -> Result<()> {
    let batch_size = 1000; // Batch size for processing
    let mut decoder1 = AsyncDecoder::<BufReader<File>>::from_file(mbn_filepath1).await?;
    let mut decoder2 = AsyncDecoder::<BufReader<File>>::from_file(mbn_filepath2).await?;

    let mut batch1: HashMap<u64, Vec<RecordEnum>> = HashMap::new();
    let mut decoder_done = false;
    let mut unmatched_records2 = Vec::new(); // Unmatched records from file 2

    while let Some(record) = decoder2.decode_ref().await? {
        // Refill batch1 if needed
        if batch1.len() < batch_size && !decoder_done {
            while let Some(record1) = decoder1.decode_ref().await? {
                let record_enum = RecordEnum::from_ref(record1)?;
                // println!("{:?}", record_enum);
                let ts_event = record_enum.header().ts_event;
                batch1.entry(ts_event).or_default().push(record_enum);
            }
            if batch1.is_empty() {
                decoder_done = true; // No more records to process
            }
        }

        // Process current record from file 2
        let record_enum = RecordEnum::from_ref(record)?;
        let ts_event = record_enum.header().ts_event;

        if let Some(group) = batch1.get_mut(&ts_event) {
            // Try to match within the group
            if let Some(pos) = group.iter().position(|r| r == &record_enum) {
                group.remove(pos); // Remove matched record
                if group.is_empty() {
                    batch1.remove(&ts_event); // Remove empty groups
                }
            } else {
                unmatched_records2.push(record_enum); // No match found
            }
        } else {
            unmatched_records2.push(record_enum); // No group for ts_event
        }
    }

    // Write unmatched records to an output file
    write_unmatched_records("compare_results.txt", &batch1, &unmatched_records2).await?;

    // Print match status
    if batch1.is_empty() && unmatched_records2.is_empty() {
        println!("All records match successfully.");
    } else {
        eprintln!("Unmatched records detected. Check the output file: compare_results.txt");
    }

    Ok(())
}

async fn write_unmatched_records(
    output_file: &str,
    unmatched_batch1: &HashMap<u64, Vec<RecordEnum>>,
    unmatched_records2: &[RecordEnum],
) -> Result<()> {
    let mut file = File::create(output_file).await?;

    // Write unmatched records from file 1
    if !unmatched_batch1.is_empty() {
        file.write_all(b"Unmatched MBN File 1 Records:\n").await?;
        for (ts_event, records) in unmatched_batch1 {
            for record in records {
                file.write_all(format!("{:?} (ts_event: {})\n", record, ts_event).as_bytes())
                    .await?;
            }
        }
    }

    // Write unmatched records from file 2
    if !unmatched_records2.is_empty() {
        file.write_all(b"Unmatched MBN File 2 Records:\n").await?;
        for record in unmatched_records2 {
            file.write_all(format!("{:?}\n", record).as_bytes()).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::vendors::v_databento::{
        extract::read_dbn_file,
        transform::{instrument_id_map, to_mbn},
    };
    use mbn::metadata::Metadata;
    use mbn::{
        enums::{Dataset, Schema},
        symbols::SymbolMap,
    };

    async fn dummy_file() -> Result<PathBuf> {
        // Load DBN file
        let file_path = PathBuf::from(
            "tests/data/databento/GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
        );

        let (mut decoder, map) = read_dbn_file(file_path).await?;

        // MBN instrument map
        let mut mbn_map = HashMap::new();
        mbn_map.insert("ZM.n.0".to_string(), 20 as u32);
        mbn_map.insert("GC.n.0".to_string(), 21 as u32);
        // Map DBN instrument to MBN insturment
        let new_map = instrument_id_map(map, mbn_map)?;

        // Test
        let metadata = Metadata::new(Schema::Mbp1, Dataset::Futures, 0, 0, SymbolMap::new());
        let mbn_file_name =
            PathBuf::from("tests/data/compare_ZM.n.0_GC.n.0_mbp-1_2024-08-20_2024-08-20.bin");

        let _ = to_mbn(&metadata, &mut decoder, &new_map, &mbn_file_name).await?;

        Ok(mbn_file_name)
    }

    #[tokio::test]
    #[serial_test::serial]
    // #[ignore]
    async fn test_compare_mbn_equal() -> Result<()> {
        let path = dummy_file().await?;

        // Test
        let result = compare_mbn(&path, &path).await?;

        // Validate
        assert!(result == ());

        //Cleanup
        if path.exists() {
            std::fs::remove_file(&path).expect("Failed to delete the test file.");
        }

        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    // #[ignore]
    async fn test_compare_mbn_unequal() -> Result<()> {
        let mbn_file_path1 = PathBuf::from("tests/data/midas/bbo1m_test.bin");
        let path = dummy_file().await?;

        // Test
        let x = compare_mbn(&mbn_file_path1, &path).await?;

        // Validate
        assert!(x == ());

        //Cleanup
        if path.exists() {
            std::fs::remove_file(&path).expect("Failed to delete the test file.");
        }

        Ok(())
    }
}
