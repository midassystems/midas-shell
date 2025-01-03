use colored::Colorize;
use console::measure_text_width;

pub(crate) fn print_header() {
    println!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
        " ============================================================================="
            .cyan()
            .bold(),
        "                    __  __  _____  _____       _      ____",
        "                   |  \\/  ||_   _||  __ \\     / \\    /  __|",
        "                   | \\  / |  | |  | |  | |   / _ \\   \\  \\ ",
        "                   | |\\/| |  | |  | |  | |  / ___ \\   \\  \\ ",
        "                   | |  | | _| |_ | |__| | / /   \\ \\  _\\  \\",
        "                   |_|  |_||_____||_____/ /_/     \\_\\|____/",
        " ============================================================================="
            .cyan()
            .bold(),
    );
}

pub(crate) fn print_menu() {
    let arrow = ">    ".bright_magenta();
    let lines = vec![
        format!(
            "{}{}         {}",
            arrow,
            "Historical".cyan(),
            "Download historical price data.".purple()
        ),
        format!(
            "{}{}         {}",
            arrow,
            "Instrument".cyan(),
            "Create instruments.".purple()
        ),
        format!(
            "{}{}           {}",
            arrow,
            "Strategy".cyan(),
            "Strategy related commands.".purple()
        ),
        format!(
            "{}{}               {}",
            arrow,
            "Live".cyan(),
            "Start live trading with a specified strategy".purple(),
        ),
        format!(
            "{}{}            {}",
            arrow,
            "Backest".cyan(),
            "Run a backtest with a specified strategy".purple(),
        ),
        format!(
            "{}{}          {}",
            arrow,
            "Dashboard".cyan(),
            "Open the trading dashboard".purple()
        ),
        format!(
            "{}{}              {}",
            arrow,
            "Midas".cyan(),
            "Commands for Midas files.".purple()
        ),
        format!(
            "{}{}          {}",
            arrow,
            "Databento".cyan(),
            "Commands for Databento source.".purple()
        ),
        format!(
            "{}{}             {}",
            arrow,
            "Clear".cyan(),
            " Clear the terminal window".purple(),
        ),
        format!(
            "{}{}               {}",
            arrow,
            "Help".cyan(),
            "Show the help menu".purple()
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
