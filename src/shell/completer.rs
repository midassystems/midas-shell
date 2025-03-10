use crate::shell::commands::ShellArgs;
use clap::{Command, CommandFactory};
use reedline::{Completer, Span, Suggestion};

/// Main REPL completer struct
pub(crate) struct ReplCompleter {
    clap_command: Command, // Clap Command to handle subcommand discovery
}

impl Completer for ReplCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        let mut completions = vec![];
        completions.extend(if line.contains(' ') {
            let words: Vec<&str> = line[0..pos].split_whitespace().collect();

            // Find the "deepest" subcommand in the line
            let mut deepest_command: Option<&Command> = None;
            let mut deepest_command_idx = 0;
            for (i, word) in words.iter().enumerate() {
                if let Some(nearest) = deepest_command {
                    if let Some(subcommand) = nearest.find_subcommand(word) {
                        deepest_command = Some(subcommand);
                        deepest_command_idx = i;
                    }
                } else {
                    // Check if it's a top-level command
                    deepest_command = self.clap_command.find_subcommand(word);
                    deepest_command_idx = i;
                }
            }

            let words_left = &words[deepest_command_idx..];
            let mut words_rev = words_left.iter().rev();

            if let Some(command) = deepest_command {
                let last_word = words_rev.next().unwrap();
                let last_word_start_pos = line.len() - last_word.len();
                let span = Span::new(last_word_start_pos, pos);
                self.parameter_values_starting_with(command, last_word, span)
            } else {
                vec![]
            }
        } else {
            let span = Span::new(0, pos);
            self.commands_starting_with(line, span)
        });
        completions.dedup();
        completions
    }
}

impl ReplCompleter {
    /// Create a new `ReplCompleter` from the `CliArgs` structure.
    pub fn new() -> Self {
        // Use the `CliArgs::command()` to generate the top-level `clap::Command` structure
        let clap_command = ShellArgs::command();

        ReplCompleter { clap_command }
    }

    fn build_suggestion(
        &self,
        value: &str,
        _description: Option<&clap::builder::StyledStr>, // Use StyledStr here
        span: Span,
    ) -> Suggestion {
        Suggestion {
            value: value.to_string(),
            description: None, // Convert StyledStr to String
            extra: None,
            span,
            style: None, // Add style field as None
            append_whitespace: true,
        }
    }

    /// Get parameter values starting with a specific prefix
    fn parameter_values_starting_with(
        &self,
        command: &Command,
        search: &str,
        span: Span,
    ) -> Vec<Suggestion> {
        let mut completions = vec![];

        for subcommand in command.get_subcommands() {
            if subcommand.get_name().starts_with(search) {
                completions.push(self.build_suggestion(
                    subcommand.get_name(),
                    subcommand.get_about(),
                    span,
                ));
            }
        }

        completions
    }

    /// Get top-level commands starting with a specific prefix
    fn commands_starting_with(&self, search: &str, span: Span) -> Vec<Suggestion> {
        let result: Vec<Suggestion> = self
            .clap_command
            .get_subcommands()
            .filter(|cmd| cmd.get_name().starts_with(search))
            .map(|cmd| self.build_suggestion(cmd.get_name(), cmd.get_about(), span))
            .collect();

        result
    }
}
