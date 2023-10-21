// Our basic error type.

use std::{
    error::{self, Error as _},
    fmt, result,
};

use anstream::eprintln;
use async_rusqlite::AlreadyClosed;
use codespan_reporting::{
    diagnostic::Diagnostic,
    files::SimpleFiles,
    term::{
        self,
        termcolor::{ColorChoice, StandardStream},
    },
};
use owo_colors::OwoColorize;

/// Our standard result type.
pub type Result<T, E = Error> = result::Result<T, E>;

/// Our standard error type.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// An error occurred in SQL source code supplied by the user, either at
    /// parse time or later.
    Source(Box<SourceError>),

    /// Two tables were not equal.
    TablesNotEqual { message: String },

    /// An error with extra context. We may replace this with more specific
    /// errors later.
    Context { context: String, source: Box<Error> },

    /// An unknown error occurred.
    Other(Box<dyn error::Error + Send + Sync + 'static>),
}

impl Error {
    /// Create a new `Error::TablesNotEqual`.
    pub fn tables_not_equal(message: impl Into<String>) -> Self {
        Error::TablesNotEqual {
            message: message.into(),
        }
    }

    /// Create a new `Error::Other` from an error value.
    pub fn other<E>(e: E) -> Self
    where
        E: error::Error + Send + Sync + 'static,
    {
        Error::Other(Box::<E>::new(e))
    }

    /// Emit this error to stderr. This does extra formatting for `SourceError`,
    /// with colors and source code snippets.
    pub fn emit(&self) {
        match self {
            Error::Source(e) => {
                e.emit();
            }
            _ => {
                let first = if self.is_transparent() {
                    self.source().unwrap()
                } else {
                    self
                };
                eprintln!("{} {}", "ERROR:".red(), first);
                let mut current = first;
                while let Some(next) = current.source() {
                    current = next;
                    if let Some(source) = current.downcast_ref::<Error>() {
                        if source.is_transparent() {
                            continue;
                        }
                    }
                    eprintln!("  {} {}", "caused by:".red(), current);
                }
            }
        }
    }

    /// Should this error be treated as "transparent" in error output?
    pub fn is_transparent(&self) -> bool {
        match self {
            Error::Source(_) | Error::Other(_) => true,
            Error::TablesNotEqual { .. } | Error::Context { .. } => false,
        }
    }
}

impl From<SourceError> for Error {
    fn from(e: SourceError) -> Self {
        Error::Source(Box::new(e))
    }
}

impl From<AlreadyClosed> for Error {
    fn from(e: AlreadyClosed) -> Self {
        Error::Context {
            context: "SQLite3 worker thread exited unexpectedly".to_string(),
            source: Box::new(e.into()),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Source(e) => e.fmt(f),
            Error::TablesNotEqual { message } => write!(f, "{}", message),
            Error::Context { context, .. } => write!(f, "{}", context),
            Error::Other(e) => e.fmt(f),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::Source(e) => Some(e.as_ref()),
            Error::Other(e) => Some(e.as_ref()),
            Error::Context { source, .. } => Some(source.as_ref()),
            Error::TablesNotEqual { .. } => None,
        }
    }
}

/// Format an error message.
macro_rules! format_err {
    ($($arg:tt)*) => {
        Error::Other(format!($($arg)*).into())
    };
}

// A trick to make `format_err!` accessible from within this crate.
pub(crate) use format_err;

/// Helper trait used
pub trait Context<T, E>: Sized {
    fn with_context<S, F>(self, context_fn: F) -> Result<T>
    where
        S: Into<String>,
        F: FnOnce() -> S,
    {
        self.context(context_fn())
    }

    fn context<S>(self, context: S) -> Result<T>
    where
        S: Into<String>;
}

impl<T, E> Context<T, E> for Result<T, E>
where
    E: error::Error + Send + Sync + 'static,
{
    fn context<S>(self, context: S) -> Result<T>
    where
        S: Into<String>,
    {
        self.map_err(|e| Error::Context {
            context: context.into(),
            source: Box::new(Error::other(e)),
        })
    }
}

#[derive(Debug)]
pub struct SourceError {
    pub expected: String,
    pub files: SimpleFiles<String, String>,
    pub diagnostic: Diagnostic<usize>,
}

impl SourceError {
    pub fn emit(&self) {
        let writer = StandardStream::stderr(ColorChoice::Auto);
        let config = term::Config::default();
        term::emit(&mut writer.lock(), &config, &self.files, &self.diagnostic)
            .expect("could not write to stderr");
    }
}

impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "parser error: expected {}", self.expected)
    }
}

impl error::Error for SourceError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}
