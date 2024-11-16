use crate::error::Error;
use crate::error::Result;
use midas_client::historical::Historical;
use std::env;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

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

/// Loads to database, deletes MBN file after
pub async fn load_file<T: AsRef<Historical>>(file_name: &PathBuf, client: T) -> Result<()> {
    // println!("{:?}", file_name);
    // let filepath = &PathBuf::from("data/processed_data").join(file_name);

    // Convert PathBuf to String
    let file_string: String = file_name.to_string_lossy().into_owned();

    // -- Load
    let _ = client.as_ref().create_mbp_from_file(&file_string).await?;

    // // -- CLEAN
    // if let Err(e) = tokio::fs::remove_file(&file_name).await {
    //     eprintln!("Failed to delete MBN file: {:?}", e);
    // } else {
    //     println!("MBN file deleted successfully: {:?}", file_name);
    // }
    Ok(())
}

pub fn user_input() -> Result<bool> {
    // Check if running in a non-interactive mode (like in a cron job)
    if std::env::var("NON_INTERACTIVE").is_ok() {
        println!("Non-interactive mode detected. Proceeding with default behavior.");
        return Ok(true); // Default to proceeding or canceling based on your requirement
    }

    let mut attempts = 0; // Initialize a counter for attempts

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

pub fn run_python_engine(config_path: &str) -> std::io::Result<()> {
    // Determine the Python executable to use from the current environment
    let python_bin = which_python()?;

    // Instead of specifying a script path, use the `-m` flag to run the midas module directly
    let status = Command::new(python_bin)
        .arg("-m") // This tells Python to run the specified module
        .arg("midas.cli") // Specify the module you want to run
        .arg(config_path) // Pass the config_path argument
        .status()?;

    if status.success() {
        println!("Python engine ran successfully.");
    } else {
        eprintln!("Python engine encountered an error.");
    }

    Ok(())
}

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
