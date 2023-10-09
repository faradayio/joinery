// Our basic error type.

use std::{
    error::{self, Error as _},
    fmt, result,
};

use codespan_reporting::{
    diagnostic::Diagnostic,
    files::SimpleFile,
    term::{
        self,
        termcolor::{ColorChoice, StandardStream},
    },
};
use thiserror::Error;

/// Our standard result type.
pub type Result<T, E = Error> = result::Result<T, E>;

/// Our standard error type.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// An error occurred in SQL source code supplied by the user, either at
    /// parse time or later.
    #[error(transparent)]
    Source(#[from] Box<SourceError>),

    /// An error with extra context. We may replace this with more specific
    /// errors later.
    #[error("{context}")]
    Context {
        context: String,
        #[source]
        source: Box<Error>,
    },

    /// An unknown error occurred.
    #[error(transparent)]
    Other(Box<dyn error::Error + Send + Sync + 'static>),
}

impl Error {
    /// Create a new `Error::Other` from an error value.
    fn other<E>(e: E) -> Self
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
            e => {
                // Skip transparent errors.
                let next = match e {
                    Error::Source(e) => e.source(),
                    Error::Other(e) => e.source(),
                    Error::Context { .. } => Some::<&dyn error::Error>(e),
                }
                .expect("should always have a source error");

                // Print out the error and its causes.
                eprintln!("ERROR: {}", next);
                while let Some(next) = next.source() {
                    eprintln!("  caused by: {}", next);
                }
            }
        }
    }
}

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
    pub source: peg::error::ParseError<peg::str::LineCol>,
    pub files: SimpleFile<&'static str, String>,
    pub diagnostic: Diagnostic<()>,
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
        write!(f, "parser error: expected {}", self.source)
    }
}

impl error::Error for SourceError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(&self.source)
    }
}
