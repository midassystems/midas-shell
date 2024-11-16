// prompt.rs
use reedline::{Prompt, PromptEditMode, PromptHistorySearch};
use std::borrow::Cow;
use std::env;
use std::path::PathBuf;
use std::process::Command;

#[derive(Debug)]
pub struct MidasPrompt {
    use_starship: bool,
}
impl MidasPrompt {
    pub fn new(use_starship: bool) -> Self {
        MidasPrompt { use_starship }
    }

    // Function to get the default prompt if Starship isn't used
    fn default_prompt(&self) -> String {
        "Midas Shell\n=> ".to_string()
    }

    fn starship_prompt(&self) -> String {
        let config_path = self.load_starship_config();

        // Set custom Starship config if needed
        env::set_var("STARSHIP_CONFIG", config_path);
        env::set_var("STARSHIP_SHELL", "sh"); // Try 'bash' or 'sh'

        // Run Starship and capture the output
        let output = Command::new("starship")
            .arg("prompt")
            .output()
            .unwrap_or_else(|_| {
                // Fallback in case Starship fails to run
                Command::new("echo")
                    .arg(self.default_prompt())
                    .output()
                    .expect("Failed to execute fallback prompt")
            });

        // Convert output to a String
        String::from_utf8_lossy(&output.stdout).to_string()
    }

    fn load_starship_config(&self) -> PathBuf {
        let config_path: PathBuf;

        if cfg!(test) {
            config_path = PathBuf::from("../config/midas_starship.toml");
        } else {
            // Check if we're in a development environment (optional)
            if std::env::var("RUST_ENV").unwrap_or_default() == "dev" {
                let exe_dir = std::env::current_exe().expect("Unable to get executable directory");
                let exe_dir = exe_dir
                    .parent()
                    .expect("Unable to find parent directory of executable");

                // Attempt to find the config file in the same directory as the executable
                config_path = exe_dir.join("config/midas_starship.toml");
            } else {
                // For production, check the user's config directory
                let home_dir = std::env::var("HOME").expect("Unable to get HOME directory");
                config_path =
                    PathBuf::from(format!("{}/.config/midas/midas_starship.toml", home_dir));
            }
        }
        config_path
    }
}

impl Prompt for MidasPrompt {
    fn render_prompt_left(&self) -> Cow<'_, str> {
        // Check if we should use the Starship prompt or default
        let prompt = if self.use_starship {
            self.starship_prompt()
        } else {
            self.default_prompt()
        };
        Cow::Owned(prompt) // Using an owned string for the left prompt
    }

    fn render_prompt_right(&self) -> Cow<'_, str> {
        Cow::Borrowed("")
    }

    fn render_prompt_indicator(&self, _mode: PromptEditMode) -> Cow<'_, str> {
        Cow::Borrowed("")
    }

    fn render_prompt_multiline_indicator(&self) -> Cow<'_, str> {
        Cow::Borrowed("::: ")
    }

    fn render_prompt_history_search_indicator(&self, _mode: PromptHistorySearch) -> Cow<'_, str> {
        Cow::Borrowed("(search)> ")
    }
}
