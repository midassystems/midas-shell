use crate::context::Context;
use crate::utils::{get_dashboard_path, run_python_engine};
use crate::vendors::midas::checks::find_duplicates;
use crate::vendors::midas::compare::compare_mbinary;
use crate::vendors::{databento::compare::compare_dbn, DownloadType, Vendor};
use mbinary::enums::Dataset;
use mbinary::params::RetrieveParams;
use mbinary::symbols::Instrument;
use mbinary::vendors::Vendors;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;
use time::OffsetDateTime;

#[derive(Debug, Deserialize)]
struct StrategyConfig {
    name: String,
}

pub struct TaskManager {
    context: Context,
    processes: HashMap<u32, Child>,
}

impl TaskManager {
    pub fn new(context: Context) -> TaskManager {
        TaskManager {
            context,
            processes: HashMap::new(),
        }
    }

    pub async fn list_processes(&self) {
        if self.processes.len() == 0 {
            println!("No processes currently running")
        } else {
            for (k, v) in &self.processes {
                println!("ID: {} | Process: {:?}", k, v)
            }
        }
    }

    pub async fn list_backtest(&self) {
        let client = self.context.get_trading_client();

        // Call the method on the client
        let result = client.list_backtest().await;

        match result {
            Ok(backtests) => {
                if backtests.data.len() > 0 {
                    // Display the results
                    println!("{:?}", backtests.data);
                } else {
                    // Display the results
                    println!("No backtests found.");
                }
            }
            Err(e) => {
                eprintln!("Error with backtest retrieval: {:?}", e);
            }
        }
    }

    pub fn run_backtest(&self, strategy_name: &str) {
        let strategy_path = std::path::Path::new("strategies/").join(strategy_name);
        let config_path = strategy_path.join("config.toml");

        if config_path.exists() {
            println!("Backtesting strategy: {}", strategy_name);

            // Call the Python engine with the path to the config file
            if let Err(e) = run_python_engine(config_path.to_str().unwrap(), "backtest") {
                println!("Error running Python engine: {}", e);
            }
        } else {
            println!(
                "Strategy '{}' must have a config.toml at the root level.",
                strategy_name
            );
        }
    }

    pub fn run_live(&mut self, strategy_name: &str) {
        let strategy_path = std::path::Path::new("strategies/").join(strategy_name);
        let config_path = strategy_path.join("config.toml");

        if config_path.exists() {
            println!("Strategy {} going live", strategy_name);

            match run_python_engine(config_path.to_str().unwrap(), "live") {
                Ok(child) => {
                    let id = child.id();
                    self.processes.insert(id, child);
                }
                Err(e) => {
                    println!("Error running Python engine: {}", e);
                }
            }
        } else {
            println!(
                "Strategy '{}' must have a config.toml at the root level.",
                strategy_name
            );
        }
    }

    pub fn kill_process(&mut self, pid: u32) {
        match self.processes.get_mut(&pid) {
            Some(child) => {
                // Send SIGINT to Python
                let _ = signal::kill(Pid::from_raw(pid as i32), Signal::SIGINT);

                // Wait a bit for clean shutdown
                thread::sleep(Duration::from_secs(2));

                // Ensure process is fully stopped
                let _ = child.wait();

                self.processes.remove(&pid);
            }
            None => return,
        }
    }

    pub fn launch_dashboard(&self) {
        let path = match get_dashboard_path() {
            Ok(path) => path,
            Err(e) => {
                println!("Error: {:?}", e);
                return;
            }
        };

        println!("Starting the dashboard...");

        if std::env::var("RUST_ENV").unwrap_or_default() == "dev" {
            match Command::new(path).spawn() {
                Ok(_) => {}
                Err(_) => println!("Failed to start Tauri dashboard binary"),
            }
        } else {
            // For macOS, use the `open` command to launch the .app bundle
            if cfg!(target_os = "macos") {
                // Use the `open` command to launch the .app bundle
                match Command::new("open").arg(path).spawn() {
                    Ok(_) => {}
                    Err(_) => println!("Failed to start Tauri dashboard binary"),
                }
            } else if cfg!(target_os = "linux") {
                // For linux or any platform, run the binary directly
                match Command::new(path).spawn() {
                    Ok(_) => {}
                    Err(_) => println!("Failed to start Tauri dashboard binary"),
                }
            } else {
                println!("Dashboard launch is only supported on macOS in this implementation.");
            }
        }
    }

    pub async fn get_historical(&self, params: RetrieveParams, file_path: &str) {
        let client = self.context.get_historical_client();

        // Attempt to get records and save them to the file
        match client.get_records_to_file(&params, file_path).await {
            Ok(_) => {
                println!("Data successfully saved to {}", file_path);
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
    }

    pub async fn create_instrument(&self, instrument: Instrument) {
        let client = self.context.get_instrument_client();

        match client.create_symbol(&instrument).await {
            Ok(_) => {
                println!("Created symbol successfully.");
            }
            Err(e) => {
                eprintln!("{}", e);
            }
        }
    }

    pub async fn get_instruments(&self, dataset: Dataset, vendor: Option<Vendors>) {
        let client = self.context.get_instrument_client();

        let response = if vendor.is_some() {
            client.list_vendor_symbols(&vendor.unwrap(), &dataset).await
        } else {
            client.list_dataset_symbols(&dataset).await
        };

        match response {
            Ok(symbols) => {
                println!("{:?}", symbols.data);
            }
            Err(e) => {
                eprintln!("{}", e);
            }
        }
    }

    pub async fn delete_instrument(&self, instrument_id: i32) {
        let client = self.context.get_instrument_client();

        match client.delete_symbol(&instrument_id).await {
            Ok(_) => {
                println!("Successfully deleted instrument.");
            }
            Err(e) => {
                eprintln!("{}", e);
            }
        }
    }

    pub async fn update_instrument(&self, instrument: Instrument) {
        let client = self.context.get_instrument_client();

        match client.update_symbol(&instrument).await {
            Ok(_) => {
                println!("Updated symbol successfully.");
            }
            Err(e) => {
                eprintln!("{}", e);
            }
        }
    }

    pub async fn compare_mbinay_files(&self, file1: PathBuf, file2: PathBuf) {
        match compare_mbinary(&file1, &file2).await {
            Ok(_) => (),
            Err(e) => println!("Error {}", e),
        }
    }

    pub async fn check_duplicates(&self, file: PathBuf) {
        match find_duplicates(&file).await {
            Ok(_) => (),
            Err(e) => println!("Error {}", e),
        }
    }

    pub async fn list_strategies(&self) {
        // Logic for listing strategies
        let strategy_dir = Path::new("strategies/");
        if strategy_dir.exists() && strategy_dir.is_dir() {
            for entry in fs::read_dir(strategy_dir).unwrap() {
                let entry = entry.unwrap();
                if entry.path().is_dir() {
                    let config_path = entry.path().join("config.toml");
                    if config_path.exists() {
                        match fs::read_to_string(&config_path) {
                            Ok(config_content) => {
                                match toml::from_str::<StrategyConfig>(&config_content) {
                                    Ok(config) => {
                                        println!("  - {}", config.name);
                                    }
                                    Err(e) => {
                                        println!("Error parsing TOML in {:?}: {}", config_path, e);
                                    }
                                }
                            }
                            Err(e) => {
                                println!("Error reading file {:?}: {}", config_path, e);
                            }
                        }
                    } else {
                        println!("Warning: No config.toml found in {:?}", entry.path());
                    }
                }
            }
        } else {
            println!("No strategies found or 'strategies/' directory does not exist.");
        }
    }

    // Vendors
    pub async fn update(&self, dataset: Dataset, approval: bool) {
        let db_client = self.context.get_databento_client();
        let hist_client = self.context.get_historical_client();
        let inst_client = self.context.get_instrument_client();

        let mut db_client = db_client.lock().await;

        match db_client
            .update(dataset, &hist_client, &inst_client, approval)
            .await
        {
            Ok(_) => (),
            Err(e) => println!("Error {}", e),
        }
    }

    pub async fn download(
        &self,
        tickers: &Vec<String>,
        schema: &dbn::Schema,
        dataset: &dbn::Dataset,
        stype: &dbn::SType,
        start: OffsetDateTime,
        end: OffsetDateTime,
        approval: bool,
        dir_path: Option<String>,
    ) {
        let db_client = self.context.get_databento_client();
        let mut db_client = db_client.lock().await;

        match db_client
            .download(
                tickers,
                &schema,
                dataset,
                stype,
                start,
                end,
                approval,
                dir_path.clone(),
            )
            .await
        {
            Ok(_) => (),
            Err(e) => println!("Error {}", e),
        };
    }

    pub async fn transform(
        &self,
        dataset: Dataset,
        dbn_filepath: PathBuf,
        midas_filepath: PathBuf,
    ) {
        let db_client = self.context.get_databento_client();
        let inst_client = self.context.get_instrument_client();
        let db_client = db_client.lock().await;

        match db_client
            .transform(dataset, &dbn_filepath, &midas_filepath, &inst_client, false)
            .await
        {
            Ok(_) => (),
            Err(e) => println!("Error {}", e),
        };
    }

    pub async fn upload(
        &self,
        dataset: Dataset,
        download_type: &DownloadType,
        download_path: &PathBuf,
        mbinary_filename: &PathBuf,
    ) {
        let db_client = self.context.get_databento_client();
        let db_client = db_client.lock().await;
        let inst_client = self.context.get_instrument_client();
        let hist_client = self.context.get_historical_client();

        // Lock the mutex to get a mutable reference to DatabentoClient
        let files = match db_client
            .stage(
                dataset,
                &download_type,
                &download_path,
                &mbinary_filename,
                &inst_client,
            )
            .await
        {
            Ok(x) => x,
            Err(e) => {
                println!("Error {}", e);
                return;
            }
        };

        match db_client.upload(&hist_client, files).await {
            Ok(_) => (),
            Err(e) => println!("Error {}", e),
        };
    }

    pub async fn dbn_compare(&self, dbn_filepath: PathBuf, midas_filepath: PathBuf) {
        match compare_dbn(dbn_filepath, &midas_filepath).await {
            Ok(_) => (),
            Err(e) => println!("Error {}", e),
        };
    }
}
