use crate::error::Result;
use crate::vendors::databento::DatabentoVendor;
use midas_client::{historical::Historical, instrument::Instruments, trading::Trading};
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct Config {
    pub common: CommonConfig,
    pub vendors: VendorsConfig,
}

impl Config {
    fn from_toml(config_path: &PathBuf) -> Result<Self> {
        let config_str = std::fs::read_to_string(&config_path).unwrap_or_else(|_| {
            panic!(
                "Config file not found: {}. Please ensure it exists.",
                config_path.display()
            )
        });

        let config: Config = toml::from_str(&config_str).expect("Failed to parse config file");
        Ok(config)
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct VendorsConfig {
    pub databento_key: String,
}
impl Default for VendorsConfig {
    fn default() -> Self {
        VendorsConfig {
            databento_key: "api_key".to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct CommonConfig {
    pub log_level: String,
    pub midas_url: String,
    pub api_key: String,
}

impl Default for CommonConfig {
    fn default() -> Self {
        CommonConfig {
            log_level: "info".to_string(),
            midas_url: "http://127.0.0.1:8080".to_string(),
            api_key: "api_key".to_string(),
        }
    }
}

#[allow(dead_code)]
pub struct Context {
    config_dir: PathBuf,
    config: Config,
    historical_client: Historical,
    trading_client: Trading,
    instrument_client: Instruments,
    databento_client: Arc<Mutex<DatabentoVendor>>,
}

impl Context {
    pub fn init() -> Result<Self> {
        let config_dir = Self::config_dir();
        let config_path = Self::config_path(&config_dir);
        let config = Config::from_toml(&config_path)?;
        let historical_client = Historical::new(&config.common.midas_url);
        let trading_client = Trading::new(&config.common.midas_url);
        let instrument_client = Instruments::new(&config.common.midas_url);

        let databento_client = Arc::new(Mutex::new(DatabentoVendor::new(
            &config.vendors.databento_key,
        )?));

        Ok(Context {
            config_dir,
            config,
            historical_client,
            trading_client,
            instrument_client,
            databento_client,
        })
    }
    pub fn get_config_dir(&self) -> PathBuf {
        self.config_dir.clone()
    }

    pub fn get_config(&self) -> Config {
        self.config.clone()
    }

    pub fn get_historical_client(&self) -> Historical {
        self.historical_client.clone()
    }

    pub fn get_trading_client(&self) -> Trading {
        self.trading_client.clone()
    }

    pub fn get_instrument_client(&self) -> Instruments {
        self.instrument_client.clone()
    }

    pub fn get_databento_client(&self) -> Arc<Mutex<DatabentoVendor>> {
        Arc::clone(&self.databento_client)
    }

    /// Returns the path to the directory with all the configuration files.
    fn config_dir() -> PathBuf {
        if cfg!(test) {
            // Unit tests use a test-specific directory
            PathBuf::from("tests/config")
        } else if std::env::var("CARGO_MANIFEST_DIR").is_ok() {
            // Integration tests use the manifest directory
            PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
        } else {
            // Determine runtime environment (dev or production)
            if std::env::var("RUST_ENV").unwrap_or_default() == "dev" {
                let exe_dir = std::env::current_exe().expect("Unable to get executable directory");
                exe_dir
                    .parent()
                    .expect("Unable to find executable directory")
                    .to_path_buf()
            } else {
                // Default to the user's home `.config` directory for production
                let home_dir = std::env::var("HOME").expect("Unable to get HOME directory");
                PathBuf::from(format!("{}/.config/midas", home_dir))
            }
        }
    }
    /// Returns the path to the configuration file itself.
    fn config_path(dir: &PathBuf) -> PathBuf {
        if cfg!(test) {
            // Unit tests use a specific config file
            dir.join("config.toml")
        } else if std::env::var("CARGO_MANIFEST_DIR").is_ok() {
            // Integration tests use a specific config file
            dir.join("config_real.toml")
        } else {
            // Runtime environment
            if std::env::var("RUST_ENV").unwrap_or_default() == "dev" {
                dir.join("config_real.toml")
            } else {
                dir.join("config.toml") // Default production config file
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_config_from_toml() -> Result<()> {
        let config_path = PathBuf::from("tests/config/config.toml");

        // Test
        let _ = Config::from_toml(&config_path)?;

        Ok(())
    }

    #[tokio::test]
    async fn test_context_init() -> Result<()> {
        let config_path = PathBuf::from("tests/config/config.toml");
        let config = Config::from_toml(&config_path)?;

        // Test
        let context = Context::init()?;

        // Validate
        assert_eq!(config, context.config);

        Ok(())
    }
}
