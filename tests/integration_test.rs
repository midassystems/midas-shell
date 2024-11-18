use anyhow::Result;
use dotenv::dotenv;
use once_cell::sync::Lazy;
use repl_shell::context::Context;
use repl_shell::{self, cli::ProcessCommand};
use serial_test::serial;
use std::vec::Vec;

// Set the environment variable for test mode
const MODE: &str = "MODE";
const SYMBOLS: &str = "GC.n.0,ZM.n.0";
const START: &str = "2024-01-02";
const END: &str = "2024-01-04";
static TICKERS: Lazy<Vec<String>> = Lazy::new(|| vec!["ZM.n.0".to_string(), "GC.n.0".to_string()]);

#[tokio::test]
#[serial]
async fn test_list_strategies() -> Result<()> {
    std::env::set_var(MODE, "1");

    let context = Context::init()?;

    // Command
    let command = repl_shell::cli::strategies::StrategyCommands::List;
    command.process_command(&context).await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_list_backtests() -> Result<()> {
    let context = Context::init()?;

    // Command
    let command = repl_shell::cli::backtest::BacktestCommands::List;
    command.process_command(&context).await?;

    Ok(())
}

// NOTE: If need to test databento pulls uncomment the ignore in order, and clear the tests/data
// files
#[tokio::test]
#[serial]
#[ignore]
async fn test_update() -> Result<()> {
    std::env::set_var(MODE, "1");

    // Parameters
    let context = Context::init()?;

    let update_command = repl_shell::cli::vendors::databento::DatabentoCommands::Update {
        // tickers_filepath: Some(),
    };

    update_command.process_command(&context).await?;

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore]
async fn test_databento_download() -> Result<()> {
    std::env::set_var(MODE, "1");
    dotenv().ok();

    // Parameters
    let context = Context::init()?;

    // Mbp1
    let to_file_command = repl_shell::cli::vendors::databento::DatabentoCommands::Download {
        tickers: TICKERS.to_vec(),
        start: START.to_string(),
        end: END.to_string(),
        schema: "mbp-1".to_string(),
        dataset: "GLBX.MDP3".to_string(),
        stype: "continuous".to_string(),
        dir_path: None,
    };

    to_file_command.process_command(&context).await?;

    // Ohlcv
    let to_file_command = repl_shell::cli::vendors::databento::DatabentoCommands::Download {
        tickers: TICKERS.to_vec(),
        start: START.to_string(),
        end: END.to_string(),
        schema: "ohlcv-1h".to_string(),
        dataset: "GLBX.MDP3".to_string(),
        stype: "continuous".to_string(),
        dir_path: None,
    };

    to_file_command.process_command(&context).await?;

    // Trades
    let to_file_command = repl_shell::cli::vendors::databento::DatabentoCommands::Download {
        tickers: TICKERS.to_vec(),
        start: START.to_string(),
        end: END.to_string(),
        schema: "trades".to_string(),
        dataset: "GLBX.MDP3".to_string(),
        stype: "continuous".to_string(),
        dir_path: None,
    };

    to_file_command.process_command(&context).await?;

    // Tbbo
    let to_file_command = repl_shell::cli::vendors::databento::DatabentoCommands::Download {
        tickers: TICKERS.to_vec(),
        start: START.to_string(),
        end: END.to_string(),
        schema: "tbbo".to_string(),
        dataset: "GLBX.MDP3".to_string(),
        stype: "continuous".to_string(),
        dir_path: None,
    };

    to_file_command.process_command(&context).await?;

    // Bbo
    let to_file_command = repl_shell::cli::vendors::databento::DatabentoCommands::Download {
        tickers: TICKERS.to_vec(),
        start: START.to_string(),
        end: END.to_string(),
        schema: "bbo-1m".to_string(),
        dataset: "GLBX.MDP3".to_string(),
        stype: "continuous".to_string(),
        dir_path: None,
    };

    to_file_command.process_command(&context).await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_upload_get_compare() -> Result<()> {
    println!("Running test_databento_upload...");
    test_databento_upload().await?;
    // Clean-up intermediate file
    let _ = tokio::fs::remove_file(
        "../server/data/processed_data/ZM.n.0_GC.n.0_mbp-1_2024-01-02_2024-01-04.bin",
    )
    .await;

    println!("Running test_get_records...");
    test_get_records().await?;

    println!("Running test_compare_files...");
    test_compare_files().await?;

    Ok(())
}

async fn test_databento_upload() -> Result<()> {
    std::env::set_var(MODE, "1");
    dotenv().ok();

    // Parameters
    let context = Context::init()?;

    // Mbp1
    let upload_cmd = repl_shell::cli::vendors::databento::DatabentoCommands::Upload {
        dbn_filepath: "GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-01-02T00:00:00Z_2024-01-04T00:00:00Z.dbn"
            .to_string(),
        dbn_downloadtype: "stream".to_string(),
        mbn_filepath: "ZM.n.0_GC.n.0_mbp-1_2024-01-02_2024-01-04.bin".to_string(),
    };

    upload_cmd.process_command(&context).await?;

    Ok(())
}

async fn test_get_records() -> Result<()> {
    std::env::set_var(MODE, "1");
    dotenv().ok();

    let context = Context::init()?;

    // Mbp-1
    let schema = "mbp-1".to_string();
    let file_path = "tests/data/midas/mbp1_test.bin".to_string();

    let historical_command = repl_shell::cli::historical::HistoricalArgs {
        symbols: SYMBOLS.to_string(),
        start: START.to_string(),
        end: END.to_string(),
        schema,
        file_path,
    };

    historical_command.process_command(&context).await?;

    // Ohlcv
    let schema = "ohlcv-1h".to_string();
    let file_path = "tests/data/midas/ohlcv1h_test.bin".to_string();

    let historical_command = repl_shell::cli::historical::HistoricalArgs {
        symbols: SYMBOLS.to_string(),
        start: START.to_string(),
        end: END.to_string(),
        schema,
        file_path,
    };

    historical_command.process_command(&context).await?;

    // Trades
    let schema = "trade".to_string();
    let file_path = "tests/data/midas/trades_test.bin".to_string();

    let historical_command = repl_shell::cli::historical::HistoricalArgs {
        symbols: SYMBOLS.to_string(),
        start: START.to_string(),
        end: END.to_string(),
        schema,
        file_path,
    };

    historical_command.process_command(&context).await?;

    // Tbbo
    let schema = "tbbo".to_string();
    let file_path = "tests/data/midas/tbbo_test.bin".to_string();

    let historical_command = repl_shell::cli::historical::HistoricalArgs {
        symbols: SYMBOLS.to_string(),
        start: START.to_string(),
        end: END.to_string(),
        schema,
        file_path,
    };

    historical_command.process_command(&context).await?;

    // Bbo
    let schema = "bbo-1m".to_string();
    let file_path = "tests/data/midas/bbo1m_test.bin".to_string();

    let historical_command = repl_shell::cli::historical::HistoricalArgs {
        symbols: SYMBOLS.to_string(),
        start: START.to_string(),
        end: END.to_string(),
        schema,
        file_path,
    };

    historical_command.process_command(&context).await?;
    Ok(())
}

async fn test_compare_files() -> Result<()> {
    std::env::set_var(MODE, "1");
    dotenv().ok();

    let context = Context::init()?;

    // // Mbp-1 -- TAKES A WHILE TO RUN
    // let compare_command = repl_shell::cli::vendors::databento::DatabentoCommands::Compare {
    //     dbn_filepath:
    //         "tests/data/databento/GLBX.MDP3_mbp-1_ZM.n.0_GC.n.0_2024-01-02T00:00:00Z_2024-01-04T00:00:00Z.dbn"
    //             .to_string(),
    //     mbn_filepath: "tests/data/midas/mbp1_test.bin".to_string(),
    // };
    //
    // compare_command.process_command(&context).await?;
    //
    // Ohlcv
    let compare_command = repl_shell::cli::vendors::databento::DatabentoCommands::Compare {
        dbn_filepath:
            "tests/data/databento/GLBX.MDP3_ohlcv-1h_ZM.n.0_GC.n.0_2024-01-02T00:00:00Z_2024-01-04T00:00:00Z.dbn"
                .to_string(),
        mbn_filepath: "tests/data/midas/ohlcv1h_test.bin".to_string(),
    };

    compare_command.process_command(&context).await?;

    // Trades
    let compare_command = repl_shell::cli::vendors::databento::DatabentoCommands::Compare {
        dbn_filepath:
            "tests/data/databento/GLBX.MDP3_trades_ZM.n.0_GC.n.0_2024-01-02T00:00:00Z_2024-01-04T00:00:00Z.dbn"
                .to_string(),
        mbn_filepath: "tests/data/midas/trades_test.bin".to_string(),
    };

    compare_command.process_command(&context).await?;
    //
    // Tbbo
    let compare_command = repl_shell::cli::vendors::databento::DatabentoCommands::Compare {
        dbn_filepath:
            "tests/data/databento/GLBX.MDP3_tbbo_ZM.n.0_GC.n.0_2024-01-02T00:00:00Z_2024-01-04T00:00:00Z.dbn"
                .to_string(),
        mbn_filepath: "tests/data/midas/tbbo_test.bin".to_string(),
    };

    compare_command.process_command(&context).await?;

    // Bbo
    let compare_command = repl_shell::cli::vendors::databento::DatabentoCommands::Compare {
        dbn_filepath:
            "tests/data/databento/GLBX.MDP3_bbo-1m_ZM.n.0_GC.n.0_2024-01-02T00:00:00Z_2024-01-04T00:00:00Z.dbn"
                .to_string(),
        mbn_filepath: "tests/data/midas/bbo1m_test.bin".to_string(),
    };

    compare_command.process_command(&context).await?;

    Ok(())
}
