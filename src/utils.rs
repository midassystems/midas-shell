use crate::error::{Error, Result};
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use midas_client::historical::Historical;
use std::env;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command};
use time::{macros::time, OffsetDateTime};

/// Returns date.year()+1-01-01 00:00 or the alternate date whichever is older
pub fn get_earlier_of_year_end_or_date(
    date: OffsetDateTime,
    compare_date: OffsetDateTime,
) -> OffsetDateTime {
    // Calculate the start of the next year based on the provided date
    let next_year_start = date
        .replace_date(date.date().replace_year(date.year() + 1).unwrap())
        .replace_time(time!(00:00));

    // Return the earlier of the two dates
    next_year_start.min(compare_date)
}

pub fn date_to_unix_nanos(date_str: &str, timezone: Option<&str>) -> Result<i64> {
    // Parse the timezone or default to UTC
    let tz: Tz = match timezone {
        Some(tz_str) => tz_str
            .parse()
            .map_err(|_| Error::CustomError(format!("Invalid timezone: {}", tz_str)))?,
        None => chrono_tz::UTC,
    };

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
    // Convert to timezone-aware datetime
    let datetime_with_tz = tz
        .from_local_datetime(&naive_datetime)
        .single()
        .ok_or_else(|| {
            Error::CustomError(format!(
                "Failed to interpret '{}' in timezone '{}'",
                date_str,
                timezone.unwrap_or("UTC")
            ))
        })?;

    // // Convert to Unix time in nanoseconds
    let unix_nanos = datetime_with_tz.timestamp_nanos_opt().unwrap();

    // Convert to Unix nanoseconds
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

pub fn run_python_engine(config_path: &str, mode: &str) -> std::io::Result<Child> {
    // Determine the Python executable to use from the current environment
    let python_bin = which_python()?;

    // Instead of specifying a script path, use the `-m` flag to run the midas module directly
    let status = Command::new(python_bin)
        .arg("-m") // This tells Python to run the specified module
        .arg("midastrader.cli") // Specify the module you want to run
        .arg(config_path) // Pass the config_path argument
        .arg(mode) // Backtest or Live
        // .stdout(Stdio::inherit()) // Show Python output in terminal
        // .stderr(Stdio::inherit())
        .spawn()?;

    Ok(status)
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

pub fn get_dashboard_path() -> Result<PathBuf> {
    let mut dashboard_path: PathBuf = PathBuf::new();

    // Development mode: Check for the Tauri binary in the same directory as the executable
    if std::env::var("RUST_ENV").unwrap_or_default() == "dev" {
        // Get the current executable's path
        let exe_path = std::env::current_exe().expect("Failed to get current executable path");

        // Get the directory of the current executable
        let exe_dir = exe_path.parent().expect("Failed to get parent directory");
        dashboard_path = exe_dir.to_path_buf();
        dashboard_path.push("midas-gui");
    } else {
        // Production mode
        if cfg!(target_os = "macos") {
            // In production on macOS, the Tauri app would be in /Applications
            dashboard_path = PathBuf::from("/Applications/Midas.app");
        } else if cfg!(target_os = "linux") {
            // In production on Linux, the midas-gui binary is likely installed in /usr/local/bin
            dashboard_path = PathBuf::from("/opt/midas/Midas.AppImage");
        }
    }
    if dashboard_path.exists() {
        Ok(dashboard_path)
    } else {
        println!("Midas app not found, please ensure it is installed.");
        Err(Error::CustomError("Midas app not found".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datetime_to_unix_nanos_utc() -> Result<()> {
        let date_str = "2021-11-01 01:01:01";

        // Test
        let unix_nanos = date_to_unix_nanos(date_str, None)?;

        // Validate
        assert_eq!(1635728461000000000, unix_nanos);
        Ok(())
    }

    #[test]
    fn test_date_to_unix_nanos_utc() -> Result<()> {
        let date_str = "2021-11-01";

        // Test
        let unix_nanos = date_to_unix_nanos(date_str, None)?;

        // Validate
        assert_eq!(1635724800000000000, unix_nanos);

        Ok(())
    }

    #[test]
    fn test_date_to_unix_nanos_est() -> Result<()> {
        let date_str = "2021-12-01 13:00:00";

        // Test
        let unix_nanos = date_to_unix_nanos(date_str, Some("America/New_York"))?;

        // Validate
        assert_eq!(1638381600000000000, unix_nanos);

        Ok(())
    }

    #[test]
    fn test_date_to_unix_nanos_edt() -> Result<()> {
        let date_str = "2021-07-01 13:00:00";

        // Test
        let unix_nanos = date_to_unix_nanos(date_str, Some("America/New_York"))?;

        // Validate
        assert_eq!(1625158800000000000, unix_nanos);

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
