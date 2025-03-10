pub mod commands;
pub mod completer;
pub mod header;
pub mod prompt;
pub mod table;

use crate::context::Context;
use crate::error::Result;
use crate::logger::system_logger;
use crate::TaskManager;
use clap::Parser;
use commands::clear::clear_terminal;
use commands::ShellArgs;
use completer::ReplCompleter;
use header::print_menu;
use nu_ansi_term::{Color, Style};
use prompt::MidasPrompt;
use reedline::{
    default_emacs_keybindings, ColumnarMenu, DefaultValidator, Emacs, FileBackedHistory, KeyCode,
    KeyModifiers, Keybindings, MenuBuilder, ReedlineEvent,
};
use reedline::{DefaultHinter, Reedline, ReedlineMenu, Signal};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

#[allow(dead_code)]
pub struct Repl {
    name: String,
    prompt: MidasPrompt,
    task_manager: Arc<Mutex<TaskManager>>,
    history: Option<PathBuf>,
    history_capacity: Option<usize>,
    keybindings: Keybindings,
    hinter_style: Style,
    hinter_enabled: bool,
    config_dir: PathBuf,
}

impl Repl {
    pub fn new(context: Context) -> Self {
        let name = "Repl".to_string();
        let prompt = MidasPrompt::new(false);
        let style = Style::new().italic().fg(Color::LightGray);
        let mut keybindings = default_emacs_keybindings();
        keybindings.add_binding(
            KeyModifiers::NONE,
            KeyCode::Tab,
            ReedlineEvent::Menu("completion_menu".to_string()),
        );
        let config_dir = context.get_config_dir();
        let task_manager = TaskManager::new(context);
        let task_manager = Arc::new(Mutex::new(task_manager));
        // let context = Arc::new(Mutex::new(context));

        Self {
            name,
            prompt,
            history: None,
            history_capacity: None,
            task_manager,
            keybindings,
            hinter_enabled: true,
            hinter_style: style,
            config_dir,
            // thread_pool: ThreadPool::new(10, Arc::clone(&context)),
        }
    }

    pub fn with_logger(self, log_level: String) -> Self {
        // Initialize the logger
        system_logger(&self.config_dir, log_level).expect("Failed to initialize logger");

        self
    }

    pub fn with_history(mut self, capacity: usize) -> Self {
        let path = &self.config_dir;
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
        let completer = Box::new(ReplCompleter::new());
        let completion_menu = Box::new(ColumnarMenu::default().with_name("completion_menu"));
        let validator = Box::new(DefaultValidator);

        let mut line_editor = Reedline::create()
            .with_edit_mode(Box::new(Emacs::new(self.keybindings.clone())))
            .with_completer(completer)
            .with_menu(ReedlineMenu::EngineCompleter(completion_menu))
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

    async fn parse_commands(&mut self, input: String) -> Result<()> {
        // Parse the input line as a command using Clap
        let args_result =
            ShellArgs::try_parse_from(std::iter::once("midas").chain(input.split_whitespace()));

        // Handle parsed cli
        match args_result {
            Ok(parsed_args) => {
                match parsed_args.command {
                    _ => {
                        // Process other CLI commands asynchronously
                        if let Err(err) = parsed_args
                            .command
                            .process_command(Arc::clone(&self.task_manager))
                            .await
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

        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        clear_terminal().await;
        print_menu();

        // Use a custom prompt that integrates Starship
        let mut line_editor = self.build_line_editor()?;

        loop {
            // Get input from the user using Reedline
            let sig = line_editor.read_line(&self.prompt);

            match sig {
                Ok(Signal::Success(input)) => {
                    self.parse_commands(input).await?;
                }
                Ok(Signal::CtrlD) => {
                    println!("\nExiting...");
                    break;
                }
                Ok(Signal::CtrlC) => {
                    continue;
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
