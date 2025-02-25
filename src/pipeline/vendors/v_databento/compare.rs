use super::super::super::midas::load::read_mbinary_file;
use crate::error::Result;
use crate::pipeline::vendors::v_databento::extract::read_dbn_file;
use dbn::Record as dbnRecord;
use mbinary::record_enum::RecordEnum;
use mbinary::records::Record;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub async fn compare_dbn(dbn_filepath: PathBuf, mbinary_filepath: &PathBuf) -> Result<()> {
    let batch_size = 1000; // New parameter to control batch size
    let mut mbinary_decoder = read_mbinary_file(mbinary_filepath).await?;
    let (mut dbn_decoder, _map) = read_dbn_file(dbn_filepath).await?;

    let mut mbinary_batch: HashMap<u64, Vec<RecordEnum>> = HashMap::new();
    let mut mbinary_decoder_done = false;

    // Keep track of any unmatched DBN records
    let mut unmatched_dbn_records = Vec::new();

    // Start decoding and comparing
    while let Some(dbn_record) = dbn_decoder.decode_record_ref().await? {
        // If MBN batch is empty, refill it
        if mbinary_batch.len() < batch_size && !mbinary_decoder_done {
            while let Some(mbinary_record) = mbinary_decoder.decode_ref().await? {
                let record_enum = RecordEnum::from_ref(mbinary_record)?;
                let ts_event = record_enum.header().ts_event;
                mbinary_batch.entry(ts_event).or_default().push(record_enum);
            }
            if mbinary_batch.is_empty() {
                mbinary_decoder_done = true; // No more MBN records
            }
        }
        let dbn_record_enum = dbn_record.as_enum()?.to_owned();
        let ts_event = dbn_record_enum.header().ts_event; // Extract ts_event from DBN record

        // Check if the ts_event exists in the MBN map
        if let Some(mbinary_group) = mbinary_batch.get_mut(&ts_event) {
            // Try to find a match within the group
            if let Some(pos) = mbinary_group
                .iter()
                .position(|mbinary_record| mbinary_record == &dbn_record_enum)
            {
                mbinary_group.remove(pos); // Remove matched record
                if mbinary_group.is_empty() {
                    mbinary_batch.remove(&ts_event); // Remove the key if the group is empty
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
    if !mbinary_batch.is_empty() {
        file.write_all(b"Unmatched MBN Records:\n").await?;
        for mbinary_record in &mbinary_batch {
            file.write_all(format!("{:?}\n", mbinary_record).as_bytes())
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
    if mbinary_batch.is_empty() && unmatched_dbn_records.is_empty() {
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
        transform::{instrument_id_map, to_mbinary},
    };
    use mbinary::enums::{Dataset, Schema};
    use mbinary::metadata::Metadata;
    use mbinary::symbols::SymbolMap;

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
            PathBuf::from("tests/data/comparedbn_ZM.n.0_GC.n.0_mbp-1_2024-08-20_2024-08-20.bin");

        let _ = to_mbinary(&metadata, &mut decoder, &new_map, &mbinary_file_name).await?;

        Ok(mbinary_file_name)
    }

    #[tokio::test]
    #[serial_test::serial]
    // #[ignore]
    async fn test_compare_dbn() -> Result<()> {
        let mbinary_path = dummy_file().await?;

        let dbn_file_path = PathBuf::from(
            "tests/data/databento/GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
        );

        // Test
        let x = compare_dbn(dbn_file_path, &mbinary_path).await?;

        // Validate
        assert!(x == ());

        //Cleanup
        if mbinary_path.exists() {
            std::fs::remove_file(&mbinary_path).expect("Failed to delete the test file.");
        }

        Ok(())
    }
}
