use super::super::super::midas::load::read_mbn_file;
use crate::error::Result;
use crate::pipeline::vendors::v_databento::extract::read_dbn_file;
use databento::dbn::Record as dbnRecord;
use mbn::record_enum::RecordEnum;
use mbn::records::Record;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub async fn compare_dbn(dbn_filepath: PathBuf, mbn_filepath: &PathBuf) -> Result<()> {
    let batch_size = 1000; // New parameter to control batch size
    let mut mbn_decoder = read_mbn_file(mbn_filepath).await?;
    let (mut dbn_decoder, _map) = read_dbn_file(dbn_filepath).await?;

    let mut mbn_batch: HashMap<u64, Vec<RecordEnum>> = HashMap::new();
    let mut mbn_decoder_done = false;

    // Keep track of any unmatched DBN records
    let mut unmatched_dbn_records = Vec::new();

    // Start decoding and comparing
    while let Some(dbn_record) = dbn_decoder.decode_record_ref().await? {
        // If MBN batch is empty, refill it
        if mbn_batch.len() < batch_size && !mbn_decoder_done {
            while let Some(mbn_record) = mbn_decoder.decode_ref().await? {
                let record_enum = RecordEnum::from_ref(mbn_record)?;
                let ts_event = record_enum.header().ts_event;
                mbn_batch.entry(ts_event).or_default().push(record_enum);
            }
            if mbn_batch.is_empty() {
                mbn_decoder_done = true; // No more MBN records
            }
        }
        let dbn_record_enum = dbn_record.as_enum()?.to_owned();
        let ts_event = dbn_record_enum.header().ts_event; // Extract ts_event from DBN record

        // Check if the ts_event exists in the MBN map
        if let Some(mbn_group) = mbn_batch.get_mut(&ts_event) {
            // Try to find a match within the group
            if let Some(pos) = mbn_group
                .iter()
                .position(|mbn_record| mbn_record == &dbn_record_enum)
            {
                mbn_group.remove(pos); // Remove matched record
                if mbn_group.is_empty() {
                    mbn_batch.remove(&ts_event); // Remove the key if the group is empty
                }
            } else {
                unmatched_dbn_records.push(dbn_record_enum); // No match found in the group
            }
        } else {
            unmatched_dbn_records.push(dbn_record_enum); // No group found for the ts_event
        }
    }

    // Create or truncate the output file
    let output_file = "compare_results.txt";
    let mut file = File::create(&output_file).await?;

    // Check for remaining unmatched MBN records and write them to the file
    if !mbn_batch.is_empty() {
        file.write_all(b"Unmatched MBN Records:\n").await?;
        for mbn_record in &mbn_batch {
            file.write_all(format!("{:?}\n", mbn_record).as_bytes())
                .await?;
        }
    }

    // Check for remaining unmatched DBN records and write them to the file
    if !unmatched_dbn_records.is_empty() {
        file.write_all(b"Unmatched DBN Records:\n").await?;
        for dbn_record in &unmatched_dbn_records {
            file.write_all(format!("{:?}\n", dbn_record).as_bytes())
                .await?;
        }
    }

    // Return an error if there are unmatched records in either batch
    if mbn_batch.is_empty() && unmatched_dbn_records.is_empty() {
        println!("All records match successfully.");
    } else {
        eprintln!(
            "Unmatched records detected. Check the output file: {:?}",
            output_file
        );
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
    use mbn::enums::{Dataset, Schema};
    use mbn::metadata::Metadata;
    use mbn::symbols::SymbolMap;

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
        let metadata = Metadata::new(Schema::Mbp1, Dataset::Futures, 0, 0, SymbolMap::new());

        // Test
        let mbn_file_name =
            PathBuf::from("tests/data/comparedbn_ZM.n.0_GC.n.0_mbp-1_2024-08-20_2024-08-20.bin");

        let _ = to_mbn(&metadata, &mut decoder, &new_map, &mbn_file_name).await?;

        Ok(mbn_file_name)
    }

    #[tokio::test]
    #[serial_test::serial]
    // #[ignore]
    async fn test_compare_dbn() -> Result<()> {
        let mbn_path = dummy_file().await?;

        let dbn_file_path = PathBuf::from(
            "tests/data/databento/GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
        );

        // Test
        let x = compare_dbn(dbn_file_path, &mbn_path).await?;

        // Validate
        assert!(x == ());

        //Cleanup
        if mbn_path.exists() {
            std::fs::remove_file(&mbn_path).expect("Failed to delete the test file.");
        }

        Ok(())
    }
}
