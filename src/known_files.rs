//! Tracking of source code locations, so that we can have pretty errors.

use core::fmt;
use std::{
    fs,
    path::{Display as PathDisplay, PathBuf},
};

use codespan_reporting::files::{Error as FilesError, Files, SimpleFile};

use crate::errors::{Context, Result};

/// A file identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileId(usize);

impl fmt::Display for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// All the files we know about.
///
/// Normally, **you should only create one copy of this per "error-reporting
/// context",** including:
///
/// 1. `main`,
/// 2. a top-level server request handler, or
/// 3. a top-level test function.
///
/// And you should share it for all the files you parse. The only exception to
/// this is if you're parsing compiled-in strings, in which case you _may_
/// create a local `KnownFiles`, **only** if immediate `emit` and `panic!`
/// without returning any parse errors. This is necessary because
/// [`crate::errors::Error`] only carries a [`FileId`], but does not known what
/// `KnownFiles` it came from. We could enforce this using lifetimes, but that
/// would make things more complicated.
#[derive(Debug, Default)]
pub struct KnownFiles {
    files: Vec<FileInfo>,
}

impl KnownFiles {
    /// Create a new [`KnownFiles`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a path to the set of known files.
    pub fn add(&mut self, path: impl Into<PathBuf>) -> Result<FileId> {
        let path = path.into();
        let source = fs::read_to_string(&path)
            .with_context(|| format!("could not read file `{}`", path.display()))?;
        let file = SimpleFile::new("", source);
        let file_id = FileId(self.files.len());
        self.files.push(FileInfo { path, file });
        Ok(file_id)
    }

    /// Add a string to the set of known files. Mostly used from tests.
    pub fn add_string(&mut self, path: impl Into<PathBuf>, source: &str) -> FileId {
        let path = path.into();
        let file = SimpleFile::new("", source.to_owned());
        let file_id = FileId(self.files.len());
        self.files.push(FileInfo { path, file });
        file_id
    }

    /// Get the SQL source code for a file.
    pub fn source_code(&self, id: FileId) -> Result<&str> {
        self.source(id).context("could not get SQL source code")
    }

    /// Look up a file by ID.
    fn get_helper(&self, id: FileId) -> Result<&FileInfo, FilesError> {
        self.files.get(id.0).ok_or(FilesError::FileMissing)
    }
}

impl<'a> Files<'a> for KnownFiles {
    type FileId = FileId;

    type Name = PathDisplay<'a>;

    type Source = &'a str;

    fn name(&'a self, id: Self::FileId) -> Result<Self::Name, FilesError> {
        Ok(self.get_helper(id)?.path.display())
    }

    fn source(&'a self, id: Self::FileId) -> Result<Self::Source, FilesError> {
        Ok(self.get_helper(id)?.file.source().as_ref())
    }

    fn line_index(&'a self, id: Self::FileId, byte_index: usize) -> Result<usize, FilesError> {
        self.get_helper(id)?.file.line_index((), byte_index)
    }

    fn line_range(
        &'a self,
        id: Self::FileId,
        line_index: usize,
    ) -> Result<std::ops::Range<usize>, FilesError> {
        self.get_helper(id)?.file.line_range((), line_index)
    }
}

/// Information we know about a file.
#[derive(Debug)]
struct FileInfo {
    /// The location of the file on disk.
    path: PathBuf,
    /// We re-use this because it implements [`Files::line_index`] and
    /// [`Files::line_range`] for us. We don't use the path in it.
    file: SimpleFile<&'static str, String>,
}
