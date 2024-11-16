// use crate::error::Error;
// use std::env;
// use std::path::PathBuf;
// use std::process::Command;
//
// // Function to get the full path for the tickers.json file
// pub fn get_ticker_file() -> crate::Result<String> {
//     let home_dir = env::var("HOME").expect("HOME environment variable not set");
//     let ticker_file = PathBuf::from(home_dir).join(".config/midas/tickers.json");
//
//     // Convert PathBuf to &str
//     let ticker_file_str = ticker_file
//         .to_str()
//         .ok_or(Error::CustomError(
//             "Invalid path: non-UTF-8 characters".to_string(),
//         ))?
//         .to_string();
//
//     Ok(ticker_file_str)
// }
//
// pub fn run_python_engine(config_path: &str) -> std::io::Result<()> {
//     // Determine the Python executable to use from the current environment
//     let python_bin = which_python()?;
//
//     // Instead of specifying a script path, use the `-m` flag to run the midas module directly
//     let status = Command::new(python_bin)
//         .arg("-m") // This tells Python to run the specified module
//         .arg("midas.cli") // Specify the module you want to run
//         .arg(config_path) // Pass the config_path argument
//         .status()?;
//
//     if status.success() {
//         println!("Python engine ran successfully.");
//     } else {
//         eprintln!("Python engine encountered an error.");
//     }
//
//     Ok(())
// }
//
// fn which_python() -> std::io::Result<PathBuf> {
//     // Use `which` or `where` command to find Python in the current environment's PATH
//     let python_command = if cfg!(target_os = "windows") {
//         "where"
//     } else {
//         "which"
//     };
//
//     let output = Command::new(python_command).arg("python").output();
//
//     match output {
//         Ok(output) if output.status.success() => {
//             // Extract the first line from the output, which should be the path to Python
//             let path = String::from_utf8_lossy(&output.stdout)
//                 .lines()
//                 .next()
//                 .map(PathBuf::from);
//
//             if let Some(python_path) = path {
//                 return Ok(python_path);
//             }
//
//             Err(std::io::Error::new(
//                 std::io::ErrorKind::NotFound,
//                 "Python executable not found in the current environment",
//             ))
//         }
//         _ => Err(std::io::Error::new(
//             std::io::ErrorKind::NotFound,
//             "Python executable not found in the current environment",
//         )),
//     }
// }
