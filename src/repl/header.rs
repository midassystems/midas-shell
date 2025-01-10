use colored::Colorize;
use console::measure_text_width;

pub(crate) fn print_header() {
    println!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
        " ============================================================================="
            .green()
            .bold(),
        "                    __  __  _____  _____       _      ____",
        "                   |  \\/  ||_   _||  __ \\     / \\    /  __|",
        "                   | \\  / |  | |  | |  | |   / _ \\   \\  \\ ",
        "                   | |\\/| |  | |  | |  | |  / ___ \\   \\  \\ ",
        "                   | |  | | _| |_ | |__| | / /   \\ \\  _\\  \\",
        "                   |_|  |_||_____||_____/ /_/     \\_\\|____/",
        " ============================================================================="
            .green()
            .bold(),
    );
}

pub(crate) fn print_menu() {
    let arrow = "  ";
    let lines = vec![
        format!(
            "{}{}         {}",
            arrow,
            "Historical".green(),
            "Download historical price data.".white()
        ),
        format!(
            "{}{}         {}",
            arrow,
            "Instrument".green(),
            "Create instruments.".white()
        ),
        format!(
            "{}{}           {}",
            arrow,
            "Strategy".green(),
            "Strategy related commands.".white()
        ),
        format!(
            "{}{}               {}",
            arrow,
            "Live".green(),
            "Start live trading with a specified strategy".white(),
        ),
        format!(
            "{}{}            {}",
            arrow,
            "Backest".green(),
            "Run a backtest with a specified strategy".white(),
        ),
        format!(
            "{}{}          {}",
            arrow,
            "Dashboard".green(),
            "Open the trading dashboard".white()
        ),
        format!(
            "{}{}              {}",
            arrow,
            "Midas".green(),
            "Commands for Midas files.".white()
        ),
        format!(
            "{}{}          {}",
            arrow,
            "Databento".green(),
            "Commands for Databento source.".white()
        ),
        format!(
            "{}{}             {}",
            arrow,
            "Clear".green(),
            " Clear the terminal window".white(),
        ),
        format!(
            "{}{}               {}",
            arrow,
            "Help".green(),
            "Show the help menu".white()
        ),
    ];

    // Define the total width for the menu box
    let total_width = 77;

    println!(
        r"
╭─────────────────────────────────── {} ────────────────────────────────────╮
│                                                                             │
│ {}                                                                   │",
        "Home".bold(),
        "Commands:".bold(),
    );

    // Print each line with dynamic padding
    for line in lines {
        let visible_width = measure_text_width(&line);
        let padding = total_width - visible_width - 2;
        println!("│ {}{} │", line, " ".repeat(padding));
    }
    println!("│                                                                             │");
    println!("╰─────────────────────────────────────────────────────────────────────────────╯");
}
