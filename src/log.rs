//! Logging and log syntax highlighting.

use colored::{ColoredString, Colorize};
use prefix::{Name, Prefix};
use std::fmt::Debug;
use std::sync::atomic::{ATOMIC_USIZE_INIT, AtomicUsize, Ordering};

static VERBOSITY: AtomicUsize = ATOMIC_USIZE_INIT;

pub const ERROR: usize = 1;
pub const INFO: usize = 2;
pub const DEBUG: usize = 3;

pub fn set_verbosity(verbosity: usize) {
    VERBOSITY.store(verbosity, Ordering::Relaxed)
}

pub fn verbosity() -> usize {
    VERBOSITY.load(Ordering::Relaxed)
}

/// Log error.
macro_rules! error {
    ($($arg:tt)*) => {
        if $crate::log::verbosity() >= $crate::log::ERROR {
            println!($($arg)*)
        }
    }
}

/// Log info.
macro_rules! info {
    ($($arg:tt)*) => {
        if $crate::log::verbosity() >= $crate::log::INFO {
            println!($($arg)*)
        }
    }
}

/// Log debug
macro_rules! debug {
    ($($arg:tt)*) => {
        if $crate::log::verbosity() >= $crate::log::DEBUG {
            println!($($arg)*)
        }
    }
}

pub fn important<T: ToString>(msg: T) -> ColoredString {
    msg.to_string().bright_yellow()
}

pub fn error<T: ToString>(msg: T) -> ColoredString {
    msg.to_string().red()
}

#[allow(unused)]
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

#[allow(unused)]
pub fn message<T: Debug>(msg: &T) -> ColoredString {
    format!("{:?}", msg).bright_magenta()
}
