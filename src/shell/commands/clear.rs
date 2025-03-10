use crate::shell::header::print_menu;
use crate::Result;
use std::fmt::Debug;

pub async fn clear_terminal() {
    if cfg!(target_os = "windows") {
        std::process::Command::new("cmd")
            .args(&["/C", "cls"])
            .status()
            .unwrap();
    } else {
        std::process::Command::new("clear").status().unwrap();
    }
}

#[derive(Debug)]
pub struct Clear;

impl Clear {
    pub async fn process_command(&self) -> Result<()> {
        clear_terminal().await;
        print_menu();

        Ok(())
    }
}
