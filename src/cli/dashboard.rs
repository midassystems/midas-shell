use crate::error;
use crate::{Error, Result};
use std::path::PathBuf;
use std::process::Command;

fn get_dashboard_path() -> Result<PathBuf> {
    let mut dashboard_path: PathBuf = PathBuf::new();

    // Development mode: Check for the Tauri binary in the same directory as the executable
    if std::env::var("RUST_ENV").unwrap_or_default() == "dev" {
        // Get the current executable's path
        let exe_path = std::env::current_exe().expect("Failed to get current executable path");

        // Get the directory of the current executable
        let exe_dir = exe_path.parent().expect("Failed to get parent directory");
        dashboard_path = exe_dir.to_path_buf();
        dashboard_path.push("midas-gui");
    } else {
        // Production mode
        if cfg!(target_os = "macos") {
            // In production on macOS, the Tauri app would be in /Applications
            dashboard_path = PathBuf::from("/Applications/Midas.app");
        } else if cfg!(target_os = "linux") {
            // In production on Linux, the midas-gui binary is likely installed in /usr/local/bin
            dashboard_path = PathBuf::from("/usr/local/bin/Midas");
        }
    }
    if dashboard_path.exists() {
        Ok(dashboard_path)
    } else {
        println!("Midas app not found, please ensure it is installed.");
        Err(Error::CustomError("Midas app not found".to_string()))
    }
}

pub fn launch_dashboard() -> Result<()> {
    let path = get_dashboard_path()?;
    println!("Starting the dashboard...");

    if std::env::var("RUST_ENV").unwrap_or_default() == "dev" {
        let _ = Command::new(path)
            .spawn()
            .map_err(|_| error!(CustomError, "Failed to start Tauri dashboard binary"))?;
    } else {
        // For macOS, use the `open` command to launch the .app bundle
        if cfg!(target_os = "macos") {
            // Use the `open` command to launch the .app bundle
            let _ = Command::new("open")
                .arg(path)
                .spawn()
                .map_err(|_| error!(CustomError, "Failed to start Tauri dashboard binary"))?;
        } else if cfg!(target_os = "linux") {
            // For linux or any platform, run the binary directly
            let _ = Command::new(path)
                .spawn()
                .map_err(|_| error!(CustomError, "Failed to start Tauri dashboard binary"))?;
        } else {
            println!("Dashboard launch is only supported on macOS in this implementation.");
        }
    }
    Ok(())
}

// pub fn launch_dashboard() {
//     // Get the current executable's path
//     let exe_path = std::env::current_exe().expect("Failed to get current executable path");
//
//     // Get the directory of the current executable
//     let exe_dir = exe_path.parent().expect("Failed to get parent directory");
//
//     let mut dashboard_path = PathBuf::from(exe_dir);
//     // For macOS, use the `open` command to launch the .app bundle
//     if cfg!(target_os = "macos") {
//         // Construct the path to the Tauri binary (direct executable)
//         dashboard_path.push("midas-gui");
//
//         // Use the `open` command to launch the .app bundle
//         let _ = Command::new(dashboard_path)
//             .spawn()
//             .expect("Failed to start Tauri dashboard");
//         // .arg(dashboard_path)
//     } else if cfg!(target_os = "linux") {
//         dashboard_path.push("midas-gui");
//
//         // For linux or any platform, run the binary directly
//         let _ = Command::new(dashboard_path)
//             .spawn()
//             .expect("Failed to start Tauri dashboard binary");
//     } else {
//         println!("Dashboard launch is only supported on macOS in this implementation.");
//     }
// }

//     // For macOS, use the `open` command to launch the .app bundle
//     if cfg!(target_os = "macos") {
//         let _ = Command::new("open")
//             .arg(dashboard_path)
//             .spawn()
//             .expect("Failed to start Tauri dashboard");
//     } else {
//         println!("Dashboard launch is only supported on macOS in this implementation.");
//     }
// }
