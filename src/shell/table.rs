use prettytable::{format, Cell, Row, Table}; // Import prettytable

// Function to display a prettier table (closer to Nushell style)
pub fn display_pretty_table() {
    let mut table = Table::new();

    // Customize the table format to resemble Nushell
    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

    // Add a row with column headers
    table.add_row(Row::new(vec![
        Cell::new(" # ").style_spec("bFg"), // Bold, foreground color
        Cell::new("Name").style_spec("bFg"),
        Cell::new("Type").style_spec("bFg"),
        Cell::new("Size").style_spec("bFg"),
        Cell::new("Modified").style_spec("bFg"),
    ]));

    // Add rows of data (mimicking the structure you want)
    table.add_row(Row::new(vec![
        Cell::new("0").style_spec("Fw"),
        Cell::new("files.rs").style_spec("Fg"),
        Cell::new("File").style_spec("Fg"),
        Cell::new("4.6 KB").style_spec("Fg"),
        Cell::new("5 days ago").style_spec("Fg"),
    ]));

    table.add_row(Row::new(vec![
        Cell::new("1").style_spec("Fw"),
        Cell::new("lib.rs").style_spec("Fg"),
        Cell::new("File").style_spec("Fg"),
        Cell::new("330 B").style_spec("Fg"),
        Cell::new("5 days ago").style_spec("Fg"),
    ]));

    table.add_row(Row::new(vec![
        Cell::new("2").style_spec("Fw"),
        Cell::new("lite_parse.rs").style_spec("Fg"),
        Cell::new("File").style_spec("Fg"),
        Cell::new("6.3 KB").style_spec("Fg"),
        Cell::new("5 days ago").style_spec("Fg"),
    ]));

    table.add_row(Row::new(vec![
        Cell::new("3").style_spec("Fw"),
        Cell::new("parse.rs").style_spec("Fg"),
        Cell::new("File").style_spec("Fg"),
        Cell::new("49.8 KB").style_spec("Fg"),
        Cell::new("1 day ago").style_spec("Fg"),
    ]));

    table.add_row(Row::new(vec![
        Cell::new("4").style_spec("Fw"),
        Cell::new("path.rs").style_spec("Fg"),
        Cell::new("File").style_spec("Fg"),
        Cell::new("2.1 KB").style_spec("Fg"),
        Cell::new("5 days ago").style_spec("Fg"),
    ]));

    table.add_row(Row::new(vec![
        Cell::new("5").style_spec("Fw"),
        Cell::new("shapes.rs").style_spec("Fg"),
        Cell::new("File").style_spec("Fg"),
        Cell::new("4.7 KB").style_spec("Fg"),
        Cell::new("5 days ago").style_spec("Fg"),
    ]));

    table.add_row(Row::new(vec![
        Cell::new("6").style_spec("Fw"),
        Cell::new("signature.rs").style_spec("Fg"),
        Cell::new("File").style_spec("Fg"),
        Cell::new("1.2 KB").style_spec("Fg"),
        Cell::new("5 days ago").style_spec("Fg"),
    ]));

    // Print the table to stdout
    table.printstd();
}
