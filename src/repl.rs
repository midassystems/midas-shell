pub mod completer;
pub mod prompt;
pub mod table;

use crate::cli::dashboard::launch_dashboard;
use crate::cli::{get_commands, CliArgs};
use crate::cli::{Commands, ProcessCommand};
use crate::context::Context;
use crate::error::Result;
use crate::logger::system_logger;
use clap::Parser;
use completer::ReplCompleter;
use nu_ansi_term::{Color, Style};
use prompt::MidasPrompt;
use reedline::{
    default_emacs_keybindings, ColumnarMenu, DefaultValidator, Emacs, ExampleHighlighter,
    FileBackedHistory, KeyCode, KeyModifiers, Keybindings, MenuBuilder, ReedlineEvent,
};
use reedline::{DefaultHinter, Reedline, ReedlineMenu, Signal};
use std::path::PathBuf;

#[allow(dead_code)]
pub struct Repl {
    name: String,
    prompt: MidasPrompt,
    context: Context,
    history: Option<PathBuf>,
    history_capacity: Option<usize>,
    keybindings: Keybindings,
    hinter_style: Style,
    hinter_enabled: bool,
}

impl Repl {
    pub fn new() -> Self {
        let name = "Repl".to_string();
        let prompt = MidasPrompt::new(false);
        let context = Context::init().expect("Error with then context loading.");
        let style = Style::new().italic().fg(Color::LightGray);
        let mut keybindings = default_emacs_keybindings();
        keybindings.add_binding(
            KeyModifiers::NONE,
            KeyCode::Tab,
            ReedlineEvent::Menu("completion_menu".to_string()),
        );

        Self {
            name,
            prompt,
            history: None,
            history_capacity: None,
            context,
            keybindings,
            hinter_enabled: true,
            hinter_style: style,
        }
    }
    pub fn with_logger(self, log_level: String) -> Self {
        // Get the configuration directory from the context
        let config_dir = self.context.get_config_dir();

        // Initialize the logger
        system_logger(&config_dir, log_level).expect("Failed to initialize logger");

        self
    }

    pub fn with_history(mut self, capacity: usize) -> Self {
        let path = self.context.get_config_dir();
        let path = path.join("history.txt");
        self.history = Some(path);
        self.history_capacity = Some(capacity);

        self
    }

    pub fn with_starship_prompt(mut self) -> Self {
        self.prompt = MidasPrompt::new(true);
        self
    }

    pub fn with_hinter_style(mut self, style: Style) -> Self {
        self.hinter_style = style;

        self
    }

    /// Disables reedline's fish-style history autosuggestions
    pub fn with_hinter_disabled(mut self) -> Self {
        self.hinter_enabled = false;

        self
    }

    pub fn with_keybinding(
        mut self,
        modifier: KeyModifiers,
        key_code: KeyCode,
        command: ReedlineEvent,
    ) -> Self {
        self.keybindings.add_binding(modifier, key_code, command);

        self
    }

    fn build_line_editor(&mut self) -> Result<Reedline> {
        let cli = get_commands();

        let completer = Box::new(ReplCompleter::new());
        let highlighter = Box::new(ExampleHighlighter::new(cli.clone()));
        let completion_menu = Box::new(ColumnarMenu::default().with_name("completion_menu"));
        let validator = Box::new(DefaultValidator);

        let mut line_editor = Reedline::create()
            .with_edit_mode(Box::new(Emacs::new(self.keybindings.clone())))
            .with_completer(completer)
            .with_menu(ReedlineMenu::EngineCompleter(completion_menu))
            .with_highlighter(highlighter)
            .with_validator(validator);

        if self.hinter_enabled {
            line_editor = line_editor.with_hinter(Box::new(
                DefaultHinter::default().with_style(self.hinter_style),
            ))
        }

        if let Some(history_path) = &self.history {
            let history = FileBackedHistory::with_file(
                self.history_capacity.unwrap(),
                history_path.to_path_buf(),
            )?;
            line_editor = line_editor.with_history(Box::new(history));
        }

        Ok(line_editor)
    }

    pub async fn run(&mut self) -> Result<()> {
        // Use a custom prompt that integrates Starship
        let mut line_editor = self.build_line_editor()?;

        loop {
            // Get input from the user using Reedline
            let sig = line_editor.read_line(&self.prompt);

            match sig {
                Ok(Signal::Success(input)) => {
                    // Parse the input line as a command using Clap
                    let args_result = CliArgs::try_parse_from(
                        std::iter::once("midas").chain(input.split_whitespace()),
                    );

                    // Handle parsed cli
                    match args_result {
                        Ok(parsed_args) => {
                            match parsed_args.command {
                                // Open Midas-Gui (seperate binary)
                                Commands::Dashboard => {
                                    println!("Starting the dashboard...");
                                    launch_dashboard();
                                }
                                _ => {
                                    // Process other CLI commands asynchronously
                                    if let Err(err) =
                                        parsed_args.command.process_command(&self.context).await
                                    {
                                        // Print the error and exit the shell on fatal error
                                        eprintln!("Error:\n {}", err);
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            println!("{}", err);
                        }
                    }
                }
                Ok(Signal::CtrlD) | Ok(Signal::CtrlC) => {
                    println!("\nExiting...");
                    break;
                }
                Err(err) => {
                    println!("Error: {:?}", err);
                    break;
                }
            }
        }

        Ok(())
    }
}
