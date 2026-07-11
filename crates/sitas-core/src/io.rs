//! Polyfill for `std::io::Error` — the `ReactorBackend` trait uses it.
//! A full `std::io::Error` is not available in `no_std`; this minimal
//! replacement carries an errno-like code.

pub type Error = ErrorKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    NotFound,
    PermissionDenied,
    ConnectionRefused,
    ConnectionReset,
    ConnectionAborted,
    NotConnected,
    AddrInUse,
    AddrNotAvailable,
    BrokenPipe,
    AlreadyExists,
    WouldBlock,
    InvalidInput,
    InvalidData,
    TimedOut,
    WriteZero,
    Interrupted,
    Other,
    UnexpectedEof,
}

impl ErrorKind {
    pub fn to_string(&self) -> &'static str {
        match self {
            Self::NotFound => "not found",
            Self::WouldBlock => "would block",
            Self::TimedOut => "timed out",
            _ => "io error",
        }
    }
}

pub type Result<T> = core::result::Result<T, ErrorKind>;
