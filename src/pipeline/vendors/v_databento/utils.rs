use crate::Result;
use databento::dbn::{Dataset, Schema};
use std::path::PathBuf;
use time::{self, OffsetDateTime};

/// Create the file path for the raw download from databento, including symbols in the file name.
pub fn databento_file_path(mut filename: PathBuf, dir_path: &PathBuf, batch: bool) -> PathBuf {
    if batch {
        filename.push(format!("batch_{}", filename.to_string_lossy()));
    }
    dir_path.join("databento").join(filename)
}

pub fn databento_file_name(
    dataset: &Dataset,
    schema: &Schema,
    start: &OffsetDateTime,
    end: &OffsetDateTime,
    symbols: &Vec<String>,
    batch: bool,
) -> Result<PathBuf> {
    // Join symbols with an underscore to include in the file name
    let symbols_str = symbols.join("_");

    // Add the "batch_" prefix if batch is true
    let prefix = if batch { "batch_" } else { "" };

    let file_path = PathBuf::from(format!(
        "{}{}_{}_{}_{}_{}.dbn",
        prefix,
        dataset.as_str(),
        schema.as_str(),
        symbols_str,
        start.format(&time::format_description::well_known::Rfc3339)?,
        end.format(&time::format_description::well_known::Rfc3339)?
    ));

    Ok(file_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use databento::dbn::{Dataset, Schema};

    #[test]
    fn test_databento_file_name() -> anyhow::Result<()> {
        // Parameters
        let dataset = Dataset::GlbxMdp3;
        let start = time::macros::datetime!(2024-08-20 00:00 UTC);
        let end = time::macros::datetime!(2024-08-20 05:00 UTC);
        let symbols = vec!["ZM.n.0".to_string(), "GC.n.0".to_string()];
        let schema = Schema::Mbp1;

        // Test
        let filename = databento_file_name(&dataset, &schema, &start, &end, &symbols, false)?;

        // Validate
        let expected = PathBuf::from(
            "GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
        );
        assert_eq!(expected, filename);

        Ok(())
    }

    #[test]
    fn test_databento_file_name_batch() -> anyhow::Result<()> {
        // Parameters
        let dataset = Dataset::GlbxMdp3;
        let start = time::macros::datetime!(2024-08-20 00:00 UTC);
        let end = time::macros::datetime!(2024-08-20 05:00 UTC);
        let symbols = vec!["ZM.n.0".to_string(), "GC.n.0".to_string()];
        let schema = Schema::Mbp1;

        // Test
        let filename = databento_file_name(&dataset, &schema, &start, &end, &symbols, true)?;

        // Validate
        let expected = PathBuf::from(
            "batch_GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
        );
        assert_eq!(expected, filename);

        Ok(())
    }

    // #[test]
    // fn test_databento_file_path() -> anyhow::Result<()> {
    //     let filename = PathBuf::from(
    //         "GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
    //     );
    //
    //     let dir = PathBuf::from("test/");
    //
    //     // Test
    //     let filepath = databento_file_path(filename.clone(), &dir, false);
    //
    //     // Validate
    //     let expected = PathBuf::from("test/databento/GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn");
    //
    //     assert_eq!(filepath, expected);
    //     Ok(())
    // }
    //
    // #[test]
    // fn test_databento_batch_file_path() -> anyhow::Result<()> {
    //     let filename = PathBuf::from(
    //         "GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
    //     );
    //
    //     let dir = PathBuf::from("test/");
    //
    //     // Test
    //     let filepath = databento_file_path(filename.clone(), &dir, false);
    //
    //     // Validate
    //     let expected = PathBuf::from("test/databento/GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn");
    //
    //     assert_eq!(filepath, expected);
    //     Ok(())
    // }
}
