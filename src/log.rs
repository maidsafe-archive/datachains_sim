//! Logging and log syntax highlighting.

use colored::{ColoredString, Colorize};
use prefix::{Name, Prefix};
use std::fmt::Debug;

pub fn important<T: ToString>(msg: T) -> ColoredString {
    msg.to_string().bright_yellow()
}

pub fn error<T: ToString>(msg: T) -> ColoredString {
    msg.to_string().red()
}

pub fn name(name: &Name) -> ColoredString {
    format!("{:?}", name).bright_blue()
}

pub fn prefix(prefix: &Prefix) -> ColoredString {
    if *prefix == Prefix::EMPTY {
        "[]".bright_blue()
    } else {
        format!("[{}]", prefix).bright_blue()
    }
}

pub fn message<T: Debug>(msg: &T) -> ColoredString {
    format!("{:?}", msg).bright_magenta()
}
