use crate::Result;
use std::fmt::Debug;

#[derive(Debug)]
pub struct Exit;

impl Exit {
    pub async fn process_command(&self) -> Result<()> {
        println!("Need to add a clean up process here.");

        Ok(())
    }
}
