use comfy_table::Table;
use serde::Serialize;

/// Output format
#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(dead_code)]
pub enum OutputFormat {
    Table,
    Json,
}

/// Print items as table or JSON based on format
pub fn print_items<T: Serialize + TableRow>(items: &[T], format: OutputFormat) {
    match format {
        OutputFormat::Table => {
            if items.is_empty() {
                println!("No items found.");
                return;
            }
            let mut table = Table::new();
            table.set_header(items[0].headers());
            for item in items {
                table.add_row(item.row());
            }
            println!("{table}");
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(items).unwrap());
        }
    }
}

/// Print a single item
#[allow(dead_code)]
pub fn print_item<T: Serialize + TableRow>(item: &T, format: OutputFormat) {
    match format {
        OutputFormat::Table => {
            let headers = item.headers();
            let row = item.row();
            for (h, r) in headers.iter().zip(row.iter()) {
                println!("  {h}: {r}");
            }
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(item).unwrap());
        }
    }
}

/// Trait for items that can be displayed as a table row
pub trait TableRow {
    fn headers(&self) -> Vec<String>;
    fn row(&self) -> Vec<String>;
}

/// Print a formatted message to stdout
pub fn print_msg(msg: impl std::fmt::Display) {
    println!("{msg}");
}

/// Print an error message to stderr
pub fn print_err(msg: impl std::fmt::Display) {
    eprintln!("ERROR: {msg}");
}

/// Print a success message
pub fn print_ok(msg: impl std::fmt::Display) {
    println!("✓ {msg}");
}

/// Print an info/note message
pub fn print_note(msg: impl std::fmt::Display) {
    println!("ℹ {msg}");
}
