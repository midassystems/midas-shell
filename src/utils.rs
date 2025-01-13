use crate::error::{Error, Result};
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use midas_client::historical::Historical;
use std::env;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

pub fn date_to_unix_nanos(date_str: &str) -> Result<i64> {
    let naive_datetime = if date_str.len() == 10 {
        // Attempt to parse date-only format YYYY-MM-DD
        match NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
            Ok(naive_date) => naive_date.and_hms_opt(0, 0, 0).unwrap(), // Set time to midnight
            Err(_) => {
                return Err(Error::InvalidDateFormat(format!(
                    "Invalid date format '{}'. Expected format: YYYY-MM-DD",
                    date_str
                )));
            }
        }
    } else {
        // Attempt to parse datetime format YYYY-MM-DD HH:MM:SS
        match NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S") {
            Ok(datetime) => datetime,
            Err(_) => {
                return Err(Error::InvalidDateFormat(format!(
                    "Invalid datetime format '{}'. Expected format: YYYY-MM-DD HH:MM:SS",
                    date_str
                )));
            }
        }
    };

    // Convert the NaiveDateTime to a DateTime<Utc>
    let datetime_utc: DateTime<Utc> = DateTime::from_naive_utc_and_offset(naive_datetime, Utc);

    // Convert to Unix time in nanoseconds
    let unix_nanos = datetime_utc.timestamp_nanos_opt().unwrap();

    Ok(unix_nanos)
}

pub fn unix_nanos_to_date(unix_nanos: i64) -> Result<String> {
    // Convert the Unix nanoseconds to a DateTime<Utc>
    let datetime_utc: DateTime<Utc> = Utc.timestamp_nanos(unix_nanos);

    // Format the DateTime<Utc> to a string in the format "YYYY-MM-DD HH:MM:SS"
    let formatted_date = datetime_utc.format("%Y-%m-%d %H:%M:%S").to_string();

    Ok(formatted_date)
}

// Function to get the full path for the tickers.json file
pub fn get_ticker_file() -> crate::Result<PathBuf> {
    let ticker_path: PathBuf;

    if std::env::var("CARGO_MANIFEST_DIR").is_ok() {
        // This works for integration tests
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        ticker_path = PathBuf::from(format!("{}/tests/tickers.json", manifest_dir));
    } else {
        let home_dir = env::var("HOME").expect("HOME environment variable not set");
        ticker_path = PathBuf::from(home_dir).join(".config/midas/tickers.json");
    }

    Ok(ticker_path)
}

/// Loads a file to database, must be in the directory mounted to docker dir /data/processed_data
pub async fn load_file(file_name: &PathBuf, client: &Historical) -> Result<()> {
    let file_string: String = file_name.to_string_lossy().into_owned();
    let response = client.create_mbp_from_file(&file_string).await?;

    println!("{:?}", response);

    Ok(())
}

pub fn user_input() -> Result<bool> {
    // Check if running in a non-interactive mode (like in a cron job)
    if std::env::var("NON_INTERACTIVE").is_ok() {
        println!("Non-interactive mode detected. Proceeding with default behavior.");
        return Ok(true); // Default to proceeding or canceling based on your requirement
    }

    let mut attempts = 0;

    loop {
        // Prompt the user for input
        print!("Do you want to proceed? (y/n): ");
        std::io::stdout().flush()?; // Ensure the prompt is printed immediately

        // Read user input
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;

        // Trim and check the input
        match input.trim() {
            "y" | "Y" => {
                println!("Proceeding with the operation...");
                return Ok(true); // Return true to indicate confirmation
            }
            "n" | "N" => {
                println!("Operation canceled by the user.");
                return Ok(false); // Return false to indicate cancellation
            }
            _ => {
                println!("Invalid input. Please enter 'y' or 'n'.");
                attempts += 1; // Increment the counter for invalid input

                if attempts >= 3 {
                    println!("Too many invalid attempts. Defaulting to cancellation.");
                    return Ok(false); // Return false after 3 invalid attempts
                }
            }
        }
    }
}

pub fn run_python_engine(config_path: &str, mode: &str) -> std::io::Result<()> {
    // Determine the Python executable to use from the current environment
    let python_bin = which_python()?;

    // Instead of specifying a script path, use the `-m` flag to run the midas module directly
    let status = Command::new(python_bin)
        .arg("-m") // This tells Python to run the specified module
        .arg("midastrader.cli") // Specify the module you want to run
        .arg(config_path) // Pass the config_path argument
        .arg(mode) // Backtest or Live
        .status()?;

    if status.success() {
        println!("Python engine ran successfully.");
    } else {
        eprintln!("Python engine encountered an error.");
    }

    Ok(())
}
/// Gets python from environment.
fn which_python() -> std::io::Result<PathBuf> {
    // Use `which` or `where` command to find Python in the current environment's PATH
    let python_command = if cfg!(target_os = "windows") {
        "where"
    } else {
        "which"
    };

    let output = Command::new(python_command).arg("python").output();

    match output {
        Ok(output) if output.status.success() => {
            // Extract the first line from the output, which should be the path to Python
            let path = String::from_utf8_lossy(&output.stdout)
                .lines()
                .next()
                .map(PathBuf::from);

            if let Some(python_path) = path {
                return Ok(python_path);
            }

            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Python executable not found in the current environment",
            ))
        }
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Python executable not found in the current environment",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datetime_to_unix_nanos() -> Result<()> {
        let date_str = "2021-11-01 01:01:01";

        // Test
        let unix_nanos = date_to_unix_nanos(date_str)?;

        // Validate
        assert_eq!(1635728461000000000, unix_nanos);
        Ok(())
    }

    #[test]
    fn test_date_to_unix_nanos() -> Result<()> {
        let date_str = "2021-11-01";

        // Test
        let unix_nanos = date_to_unix_nanos(date_str)?;

        // Validate
        assert_eq!(1635724800000000000, unix_nanos);

        Ok(())
    }

    #[test]
    fn test_unix_to_date() -> Result<()> {
        let unix = 1635728461000000000;

        // Test
        let iso = unix_nanos_to_date(unix)?;

        // Validate
        assert_eq!("2021-11-01 01:01:01", iso);
        Ok(())
    }
}
