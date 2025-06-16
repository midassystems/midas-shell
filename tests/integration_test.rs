use anyhow::Result;
// use databento::dbn;
use dbn;
use dotenv::dotenv;
use mbinary::enums::{Dataset, Schema, Stype};
use mbinary::symbols::Instrument;
use mbinary::vendors::{DatabentoData, VendorData, Vendors};
use midas_client::instrument::Instruments;
use midas_clilib::backtest::BacktestCommands;
use midas_clilib::cli::commands::vendors::databento::DatabentoCommands;
use midas_clilib::context::Context;
use midas_clilib::historical::HistoricalArgs;
use midas_clilib::instrument::InstrumentCommands;
use midas_clilib::instrument::{CreateArgs, DeleteArgs, GetArgs, UpdateArgs};
use midas_clilib::midas::MidasCommands;
use midas_clilib::strategies::StrategyCommands;
use midas_clilib::TaskManager;
use midas_clilib::{self};
use serial_test::serial;
use sqlx::PgPool;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::vec::Vec;

// Set the environment variable for test mode
const MODE: &str = "MODE";
const START: &str = "2024-01-02";
const END: &str = "2024-01-04";
const SYMBOLS: [&str; 2] = ["GC.n.0", "ZM.n.0"];

// -- Helper --
async fn create_test_ticker(ticker: &str) -> Result<()> {
    let mut vendor_data = HashMap::new();
    vendor_data.insert("stype".to_string(), "continuous".to_string());
    vendor_data.insert("schema".to_string(), "mbp-1".to_string());
    vendor_data.insert("dataset".to_string(), "GLBX.MDP3".to_string());

    let create_args = CreateArgs {
        ticker: ticker.to_string(),
        name: "Test".to_string(),
        dataset: "futures".to_string(),
        vendor: "databento".to_string(),
        vendor_data,
        first_available: "2024-11-27".to_string(),
        expiration_date: "2025-01-27".to_string(),
        is_continuous: false,
        active: true,
    };

    // Command
    let command = InstrumentCommands::Create(create_args);
    let context = Context::init()?;
    let task_manager = TaskManager::new(context);
    command.process_command(task_manager).await?;

    Ok(())
}

async fn cleanup_test_ticker(ticker: String, dataset: &Dataset) -> Result<()> {
    let base_url = "http://localhost:8080";
    let client = midas_client::instrument::Instruments::new(base_url);
    let id = client.get_symbol(&ticker, dataset).await?.data[0]
        .instrument_id
        .unwrap();

    let _ = client.delete_symbol(&(id as i32)).await?;

    Ok(())
}

// -- Instrument --
#[tokio::test]
#[serial]
// #[ignore]
async fn test_create_instrument() -> Result<()> {
    let ticker = "XYZ";
    let mut vendor_data = HashMap::new();
    vendor_data.insert("stype".to_string(), "continuous".to_string());
    vendor_data.insert("schema".to_string(), "mbp-1".to_string());
    vendor_data.insert("dataset".to_string(), "GLBX.MDP3".to_string());

    // Command
    let dataset = Dataset::Futures;
    let create_args = CreateArgs {
        ticker: ticker.to_string(),
        name: "Test".to_string(),
        dataset: dataset.as_str().to_string(),
        vendor: "databento".to_string(),
        vendor_data,
        first_available: "2024-11-27".to_string(),
        expiration_date: "2025-01-27".to_string(),
        is_continuous: false,
        active: true,
    };
    let command = InstrumentCommands::Create(create_args);
    let context = Context::init()?;
    let task_manager = TaskManager::new(context);
    command.process_command(task_manager).await?;

    // Cleanup
    cleanup_test_ticker(ticker.to_string(), &dataset).await?;

    Ok(())
}
#[tokio::test]
#[serial]
// #[ignore]
async fn test_get_all_instruments() -> Result<()> {
    let ticker = "XYZ";
    let dataset = Dataset::Futures;
    let _ = create_test_ticker(ticker).await?;

    // Command
    let get_args = GetArgs {
        dataset: dataset.as_str().to_string(),
        vendor: None,
    };

    let command = InstrumentCommands::Get(get_args);
    let context = Context::init()?;
    let task_manager = TaskManager::new(context);
    command.process_command(task_manager).await?;

    // Cleanup
    cleanup_test_ticker(ticker.to_string(), &dataset).await?;

    Ok(())
}

#[tokio::test]
#[serial]
// #[ignore]
async fn test_get_instrument_by_vendor() -> Result<()> {
    let ticker = "XYZ";
    let dataset = Dataset::Futures;
    let vendor = Vendors::Databento;
    let _ = create_test_ticker(ticker).await?;

    // Command
    let get_args = GetArgs {
        dataset: dataset.as_str().to_string(),
        vendor: Some(vendor.as_str().to_string()),
    };

    let command = InstrumentCommands::Get(get_args);
    let context = Context::init()?;
    let task_manager = TaskManager::new(context);
    command.process_command(task_manager).await?;

    // Cleanup
    cleanup_test_ticker(ticker.to_string(), &dataset).await?;

    Ok(())
}

#[tokio::test]
#[serial]
// #[ignore]
async fn test_update_instrument() -> Result<()> {
    let ticker = "XYZ";
    let _ = create_test_ticker(ticker).await?;

    // Command
    let mut vendor_data = HashMap::new();
    vendor_data.insert("stype".to_string(), "continuous".to_string());
    vendor_data.insert("schema".to_string(), "mbp-1".to_string());
    vendor_data.insert("dataset".to_string(), "GLBX.MDP3".to_string());

    let dataset = Dataset::Futures;

    let args = UpdateArgs {
        instrument_id: 264,
        ticker: "ABC".to_string(),
        name: "Test2".to_string(),
        dataset: dataset.as_str().to_string(),
        vendor: "databento".to_string(),
        vendor_data,
        first_available: "2024-01-01".to_string(),
        last_available: "2024-11-01".to_string(),
        expiration_date: "2025-01-27".to_string(),
        is_continuous: false,
        active: false,
    };

    let command = InstrumentCommands::Update(args);
    let context = Context::init()?;
    let task_manager = TaskManager::new(context);
    command.process_command(task_manager).await?;

    // Cleanup
    cleanup_test_ticker(ticker.to_string(), &dataset).await?;

    Ok(())
}

#[tokio::test]
#[serial]
// #[ignore]
async fn test_delete_instrument() -> Result<()> {
    let ticker = "XYZ";
    let dataset = Dataset::Futures;
    let _ = create_test_ticker(ticker).await?;

    let base_url = "http://localhost:8080";
    let client = midas_client::instrument::Instruments::new(base_url);
    let id = client.get_symbol(&ticker.to_string(), &dataset).await?.data[0]
        .instrument_id
        .unwrap();

    // Command
    let args = DeleteArgs {
        instrument_id: id as i32,
    };

    let command = InstrumentCommands::Delete(args);
    let context = Context::init()?;
    let task_manager = TaskManager::new(context);
    command.process_command(task_manager).await?;

    Ok(())
}

// -- Strategy --
#[tokio::test]
#[serial]
// #[ignore]
async fn test_list_strategies() -> Result<()> {
    std::env::set_var(MODE, "1");

    // Command
    let command = StrategyCommands::List;
    let context = Context::init()?;
    let task_manager = TaskManager::new(context);
    command.process_command(task_manager).await?;

    Ok(())
}

// -- Backtest --
#[tokio::test]
#[serial]
// #[ignore]
async fn test_list_backtests() -> Result<()> {
    // Command
    let command = BacktestCommands::List;
    let context = Context::init()?;
    let task_manager = TaskManager::new(context);
    command.process_command(task_manager).await?;

    Ok(())
}

// -- Vendors : Databento --
#[tokio::test]
#[serial]
// #[ignore]
async fn test_upload_get_compare() -> Result<()> {
    let dataset = Dataset::Futures;

    println!("Create tickers .. ");
    create_tickers().await?;

    println!("Running test_databento_upload...");
    test_databento_upload(&dataset).await?;

    // Continuous
    println!("Running test_get_records...");
    test_get_records_continuous(&dataset).await?;

    println!("Running test_compare_files...");
    test_compare_files_continuous().await?;

    // Raw
    println!("Running test_get_records...");
    test_get_records_raw(&dataset).await?;

    println!("Running test_compare_files...");
    test_compare_files_raw().await?;

    // Cleanup
    teardown_tickers().await?;

    Ok(())
}

async fn create_tickers() -> anyhow::Result<()> {
    dotenv().ok();
    let base_url = "http://localhost:8080";
    let client = Instruments::new(&base_url);

    let schema = dbn::Schema::from_str("mbp-1")?;
    let dbn_dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
    let stype = dbn::SType::from_str("raw_symbol")?;
    let vendor_data = VendorData::Databento(DatabentoData {
        schema,
        dataset: dbn_dataset,
        stype,
    });
    let vendor = Vendors::Databento;
    let dataset = Dataset::Futures;
    let mut instruments = Vec::new();

    // LEG4
    instruments.push(Instrument::new(
        None,
        "LEG4",
        "LiveCattle-0224",
        dataset,
        vendor,
        vendor_data.encode(),
        1709229600000000000,
        1704067200000000000,
        1709229600000000000,
        false,
        true,
    ));

    // HEG4
    instruments.push(Instrument::new(
        None,
        "HEG4",
        "LeanHogs-0224",
        dataset,
        vendor,
        vendor_data.encode(),
        1707933600000000000,
        1704067200000000000,
        1707933600000000000,
        false,
        true,
    ));

    // HEJ4
    instruments.push(Instrument::new(
        None,
        "HEJ4",
        "LeanHogs-0424",
        dataset,
        vendor,
        vendor_data.encode(),
        1712941200000000000,
        1704067200000000000,
        1712941200000000000,
        false,
        true,
    ));

    // LEJ4
    instruments.push(Instrument::new(
        None,
        "LEJ4",
        "LiveCattle-0424",
        dataset,
        vendor,
        vendor_data.encode(),
        1714496400000000000,
        1704067200000000000,
        1714496400000000000,
        false,
        true,
    ));

    // HEK4
    instruments.push(Instrument::new(
        None,
        "HEK4",
        "LeanHogs-0524",
        dataset,
        vendor,
        vendor_data.encode(),
        1715706000000000000,
        1704067200000000000,
        1715706000000000000,
        false,
        true,
    ));

    // HEM4
    instruments.push(Instrument::new(
        None,
        "HEM4",
        "LeanHogs-0624",
        dataset,
        vendor,
        vendor_data.encode(),
        1718384400000000000,
        1704067200000000000,
        1718384400000000000,
        false,
        true,
    ));

    // LEM4
    instruments.push(Instrument::new(
        None,
        "LEM4",
        "LiveCattle-0624",
        dataset,
        vendor,
        vendor_data.encode(),
        1719594000000000000,
        1704067200000000000,
        1719594000000000000,
        false,
        true,
    ));

    // LEM4
    instruments.push(Instrument::new(
        None,
        "LE.c.0",
        "LiveCattle-c-0",
        dataset,
        Vendors::Internal,
        0,
        0,
        0,
        0,
        true,
        true,
    ));

    // LEM4
    instruments.push(Instrument::new(
        None,
        "LE.c.1",
        "LiveCattle-c-1",
        dataset,
        Vendors::Internal,
        0,
        0,
        0,
        0,
        true,
        true,
    ));

    // LEM4
    instruments.push(Instrument::new(
        None,
        "LE.v.0",
        "LiveCattle-v-0",
        dataset,
        Vendors::Internal,
        0,
        0,
        0,
        0,
        true,
        true,
    ));

    // LEM4
    instruments.push(Instrument::new(
        None,
        "LE.v.1",
        "LiveCattle-v-1",
        dataset,
        Vendors::Internal,
        0,
        0,
        0,
        0,
        true,
        true,
    ));

    // LEM4
    instruments.push(Instrument::new(
        None,
        "HE.c.0",
        "LeanHogs-c-0",
        dataset,
        Vendors::Internal,
        0,
        0,
        0,
        0,
        true,
        true,
    ));

    // LEM4
    instruments.push(Instrument::new(
        None,
        "HE.c.1",
        "LeanHogs-c-1",
        dataset,
        Vendors::Internal,
        0,
        0,
        0,
        0,
        true,
        true,
    ));

    // LEM4
    instruments.push(Instrument::new(
        None,
        "HE.v.0",
        "LeanHogs-v-0",
        dataset,
        Vendors::Internal,
        0,
        0,
        0,
        0,
        true,
        true,
    ));

    // LEM4
    instruments.push(Instrument::new(
        None,
        "HE.v.1",
        "LeanHogs-v-1",
        dataset,
        Vendors::Internal,
        0,
        0,
        0,
        0,
        true,
        true,
    ));

    for i in &instruments {
        let create_response = client.create_symbol(i).await?;
        let id = create_response.data as i32;
        println!("{:?} : {}", i.ticker, id);
    }

    Ok(())
}

/// Deletes the tickers created during setup
async fn teardown_tickers() -> anyhow::Result<()> {
    dotenv().ok();
    let base_url = "http://localhost:8080";
    let client = Instruments::new(&base_url);

    let tickers_to_delete = vec![
        "LEG4".to_string(),
        "HEG4".to_string(),
        "HEJ4".to_string(),
        "LEJ4".to_string(),
        "HEK4".to_string(),
        "HEM4".to_string(),
        "LEM4".to_string(),
    ];

    for ticker in tickers_to_delete {
        let response = client.get_symbol(&ticker, &Dataset::Futures).await?;
        let id = response.data[0].instrument_id.unwrap() as i32;
        client.delete_symbol(&id).await?;
        println!("Deleted ticker: {}", ticker);
    }

    Ok(())
}

async fn test_databento_upload(dataset: &Dataset) -> Result<()> {
    std::env::set_var(MODE, "1");
    dotenv().ok();

    // Parameters
    let context = Context::init()?;
    let task_manager = TaskManager::new(context);

    // Mbp1
    let upload_cmd = DatabentoCommands::Upload {
        dataset: dataset.as_str().to_string(),
        dbn_filepath:"GLBX.MDP3_mbp-1_HEG4_HEJ4_LEG4_LEJ4_LEM4_HEM4_HEK4_2024-02-09T00:00:00Z_2024-02-17T00:00:00Z.dbn".to_string(),
        dbn_downloadtype: "stream".to_string(),
        midas_filepath: "system_tests_data.bin".to_string(),
    };

    upload_cmd.process_command(task_manager).await?;

    // Update materialized view
    let database_url = "postgres://postgres:password@127.0.0.1:5434/market_data";
    let pool = PgPool::connect(database_url)
        .await
        .expect("Failed to connect to the database");

    let query = "REFRESH MATERIALIZED VIEW futures_continuous;";
    sqlx::query(query).execute(&pool).await?;

    Ok(())
}

async fn test_get_records_continuous(dataset: &Dataset) -> Result<()> {
    dotenv().ok();

    let schemas = vec![
        Schema::Mbp1,
        Schema::Tbbo,
        Schema::Trades,
        Schema::Ohlcv1S,
        Schema::Ohlcv1M,
        Schema::Ohlcv1H,
        Schema::Ohlcv1D,
        Schema::Bbo1M,
        Schema::Bbo1S,
    ];

    let tickers = vec![
        "HE.c.0".to_string(),
        "HE.c.1".to_string(),
        "LE.c.0".to_string(),
        "LE.c.1".to_string(),
    ];
    let stype = Stype::Continuous;

    for schema in &schemas {
        let context = Context::init()?;
        let task_manager = TaskManager::new(context);

        let file_path = format!(
            "tests/data/{}_{}_test.bin",
            schema.to_string(),
            stype.to_string()
        );

        let historical_command = HistoricalArgs {
            symbols: tickers.clone(),
            start: "2024-02-13 00:00:00".to_string(),
            end: "2024-02-16 00:00:00".to_string(),
            schema: schema.to_string(),
            dataset: dataset.as_str().to_string(),
            stype: stype.as_str().to_string(),
            file_path,
        };

        historical_command.process_command(task_manager).await?;
    }
    Ok(())
}

async fn test_get_records_raw(dataset: &Dataset) -> Result<()> {
    dotenv().ok();

    let schemas = vec![
        Schema::Mbp1,
        Schema::Tbbo,
        Schema::Trades,
        Schema::Ohlcv1S,
        Schema::Ohlcv1M,
        Schema::Ohlcv1H,
        Schema::Ohlcv1D,
        Schema::Bbo1M,
        Schema::Bbo1S,
    ];

    let tickers = vec![
        "LEG4".to_string(),
        "HEG4".to_string(),
        "HEJ4".to_string(),
        "LEJ4".to_string(),
        "HEK4".to_string(),
        "HEM4".to_string(),
        "LEM4".to_string(),
    ];
    let stype = Stype::Raw;

    for schema in &schemas {
        let context = Context::init()?;
        let task_manager = TaskManager::new(context);

        let file_path = format!(
            "tests/data/{}_{}_test.bin",
            schema.to_string(),
            stype.to_string()
        );

        let historical_command = HistoricalArgs {
            symbols: tickers.clone(),
            start: "2024-02-13 00:00:00".to_string(),
            end: "2024-02-17 00:00:00".to_string(),
            schema: schema.to_string(),
            dataset: dataset.as_str().to_string(),
            stype: stype.as_str().to_string(),
            file_path,
        };

        historical_command.process_command(task_manager).await?;
    }
    Ok(())
}

async fn test_compare_files_continuous() -> Result<()> {
    dotenv().ok();

    let stype = Stype::Continuous;
    let schemas = vec![
        Schema::Mbp1,
        Schema::Tbbo,
        Schema::Trades,
        Schema::Ohlcv1S,
        Schema::Ohlcv1M,
        Schema::Ohlcv1H,
        Schema::Ohlcv1D,
        Schema::Bbo1M,
        Schema::Bbo1S,
    ];
    for schema in &schemas {
        let context = Context::init()?;
        let task_manager = TaskManager::new(context);

        println!("Schema: {:?}", schema);
        let mbinary_filepath = format!(
            "tests/data/{}_{}_test.bin",
            schema.to_string(),
            stype.to_string()
        );

        let dbn_filepath = format!(
            "tests/data/databento/GLBX.MDP3_{}_HE.c.0_HE.c.1_LE.c.0_LE.c.1_2024-02-13T00:00:00Z_2024-02-16T00:00:00Z.dbn",
            schema.to_string(),
        );

        let compare_command = DatabentoCommands::Compare {
            dbn_filepath,
            midas_filepath: mbinary_filepath,
        };

        compare_command.process_command(task_manager).await?;
    }

    Ok(())
}

async fn test_compare_files_raw() -> Result<()> {
    dotenv().ok();

    let stype = Stype::Raw;
    let schemas = vec![
        Schema::Mbp1,
        Schema::Tbbo,
        Schema::Trades,
        Schema::Ohlcv1S,
        Schema::Ohlcv1M,
        Schema::Ohlcv1H,
        Schema::Ohlcv1D,
        Schema::Bbo1M,
        // Schema::Bbo1S, // 2 record not aligned appears they are missing from dbn
    ];
    for schema in &schemas {
        println!("Schema: {:?}", schema);

        let context = Context::init()?;
        let task_manager = TaskManager::new(context);

        let mbinary_filepath = format!(
            "tests/data/{}_{}_test.bin",
            schema.to_string(),
            stype.to_string()
        );

        let dbn_filepath = format!(
            "tests/data/databento/GLBX.MDP3_{}_HEG4_HEJ4_LEG4_LEJ4_LEM4_HEM4_HEK4_2024-02-13T00:00:00Z_2024-02-17T00:00:00Z.dbn",
            schema.to_string(),
        );

        let compare_command = DatabentoCommands::Compare {
            dbn_filepath,
            midas_filepath: mbinary_filepath,
        };

        compare_command.process_command(task_manager).await?;
    }

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore]
async fn test_databento_transform() -> anyhow::Result<()> {
    dotenv().ok();
    let ticker = "ZM.n.0";
    let _ = create_test_ticker(ticker).await?;

    let ticker = "GC.n.0";
    let _ = create_test_ticker(ticker).await?;

    let dataset = Dataset::Futures;

    // Mbp1
    let dbn_filepath= "tests/data/databento/GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-01-02T00:00:00Z_2024-01-04T00:00:00Z.dbn";
    let mbinary_filepath = "tests/data/ZM.n.0_GC.n.0_mbp-1_2024-01-02_2024-01-04.bin";

    let upload_cmd = DatabentoCommands::Transform {
        dataset: dataset.as_str().to_string(),
        dbn_filepath: dbn_filepath.to_string(),
        midas_filepath: mbinary_filepath.to_string(),
    };

    let context = Context::init()?;
    let task_manager = TaskManager::new(context);
    upload_cmd.process_command(task_manager).await?;

    // Check duplicates
    let duplicatecheck_cmd = MidasCommands::Duplicates {
        filepath: mbinary_filepath.to_string(),
    };

    let context = Context::init()?;
    let task_manager = TaskManager::new(context);
    duplicatecheck_cmd.process_command(task_manager).await?;

    // Check duplicates
    let compare_cmd = DatabentoCommands::Compare {
        dbn_filepath: dbn_filepath.to_string(),
        midas_filepath: mbinary_filepath.to_string(),
    };

    let context = Context::init()?;
    let task_manager = TaskManager::new(context);
    compare_cmd.process_command(task_manager).await?;

    let path = PathBuf::from(mbinary_filepath);
    if path.exists() {
        std::fs::remove_file(&path).expect("Failed to delete the test file.");
    }

    Ok(())
}

// -- Vendors : Databento --
#[tokio::test]
#[serial]
#[ignore]
async fn test_update_databento() -> Result<()> {
    dotenv().ok();

    // Set up
    let ticker1 = "HE.n.0".to_string();
    let dataset = Dataset::Futures;
    let _ = create_test_ticker(&ticker1).await?;

    // Mbp1
    let update_cmd = DatabentoCommands::Update {
        dataset: dataset.as_str().to_string(),
        approval: true,
    };

    let context = Context::init()?;
    let task_manager = TaskManager::new(context);
    update_cmd.process_command(task_manager).await?;

    // Cleaup
    let _ = cleanup_test_ticker(ticker1, &dataset).await?;

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore]
async fn test_databento_download() -> Result<()> {
    std::env::set_var(MODE, "1");
    dotenv().ok();

    let tickers: Vec<String> = SYMBOLS.iter().map(|s| s.to_string()).collect();

    // Mbp1
    let to_file_command = DatabentoCommands::Download {
        tickers: tickers.clone(),
        start: START.to_string(),
        end: END.to_string(),
        schema: "mbp-1".to_string(),
        dataset: "GLBX.MDP3".to_string(),
        stype: "continuous".to_string(),
        approval: true,
        dir_path: None,
    };

    let context = Context::init()?;
    let task_manager = TaskManager::new(context);
    to_file_command.process_command(task_manager).await?;

    // Ohlcv
    let to_file_command = DatabentoCommands::Download {
        tickers: tickers.clone(),
        start: START.to_string(),
        end: END.to_string(),
        schema: "ohlcv-1h".to_string(),
        dataset: "GLBX.MDP3".to_string(),
        stype: "continuous".to_string(),
        approval: true,
        dir_path: None,
    };

    let context = Context::init()?;
    let task_manager = TaskManager::new(context);
    to_file_command.process_command(task_manager).await?;

    // Trades
    let to_file_command = DatabentoCommands::Download {
        tickers: tickers.clone(),
        start: START.to_string(),
        end: END.to_string(),
        schema: "trades".to_string(),
        dataset: "GLBX.MDP3".to_string(),
        stype: "continuous".to_string(),
        approval: true,
        dir_path: None,
    };

    let context = Context::init()?;
    let task_manager = TaskManager::new(context);
    to_file_command.process_command(task_manager).await?;

    // Tbbo
    let to_file_command = DatabentoCommands::Download {
        tickers: tickers.clone(),
        start: START.to_string(),
        end: END.to_string(),
        schema: "tbbo".to_string(),
        dataset: "GLBX.MDP3".to_string(),
        stype: "continuous".to_string(),
        approval: true,
        dir_path: None,
    };

    let context = Context::init()?;
    let task_manager = TaskManager::new(context);
    to_file_command.process_command(task_manager).await?;

    // Bbo
    let to_file_command = DatabentoCommands::Download {
        tickers: tickers.clone(),
        start: START.to_string(),
        end: END.to_string(),
        schema: "bbo-1m".to_string(),
        dataset: "GLBX.MDP3".to_string(),
        stype: "continuous".to_string(),
        approval: true,
        dir_path: None,
    };

    let context = Context::init()?;
    let task_manager = TaskManager::new(context);
    to_file_command.process_command(task_manager).await?;

    Ok(())
}
