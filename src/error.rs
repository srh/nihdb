use std;

#[derive(Debug)]
pub struct Error {
    msg: String,
}

impl Error {
    pub fn new(msg: &str) -> Error {
        return Error{msg: msg.to_string()};
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
        return write!(f, "{}", self.msg);
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        return &self.msg;
    }

    fn cause(&self) -> Option<&std::error::Error> {
        return None;
    }
}
