use crate::error::{Error, Result};
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use std::io::Write;
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

/// Converts string date / datetime to  UNIX nanoseconds.
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

/// Converts UNIX nanoseconds to string datetime.
pub fn unix_nanos_to_date(unix_nanos: i64) -> Result<String> {
    // Convert the Unix nanoseconds to a DateTime<Utc>
    let datetime_utc: DateTime<Utc> = Utc.timestamp_nanos(unix_nanos);

    // Format the DateTime<Utc> to a string in the format "YYYY-MM-DD HH:MM:SS"
    let formatted_date = datetime_utc.format("%Y-%m-%d %H:%M:%S").to_string();

    Ok(formatted_date)
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

        //Test
        let iso = unix_nanos_to_date(unix)?;

        // Validate
        assert_eq!("2021-11-01 01:01:01", iso);
        Ok(())
    }
}
