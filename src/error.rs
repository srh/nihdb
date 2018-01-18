use std;

pub type Result<T> = std::result::Result<T, Error>;

pub fn mk_err<T>(msg: &str) -> Result<T> { Err(Error::new(msg)) }

#[derive(Debug)]
pub enum Error {
    IoError(std::io::Error),
    MsgError(String),
}

impl Error {
    pub fn new(msg: &str) -> Error {
        return Error::MsgError(msg.to_string());
    }

    pub fn msg(&self) -> &str {
        return match self {
            &Error::IoError(ref e) => (e as &std::error::Error).description(),
            &Error::MsgError(ref s) => &s,
        };
    }
}

pub trait OptionExt<T> {
    fn or_err(self, msg: &str) -> std::result::Result<T, Error>;
}

impl<T> OptionExt<T> for Option<T> {
    fn or_err(self, msg: &str) -> std::result::Result<T, Error> {
        if let Some(x) = self {
            return Ok(x);
        }
        return Err(Error::new(msg));
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        return write!(f, "{}", self.msg());
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str { self.msg() }

    fn cause(&self) -> Option<&std::error::Error> {
        return match self {
            &Error::IoError(ref e) => Some(e),
            &Error::MsgError(_) => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error { Error::IoError(e) }
}
