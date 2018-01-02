use std;

#[derive(Debug)]
pub struct RihError {
    msg: String,
}

impl RihError {
    pub fn new(msg: &str) -> RihError {
        return RihError{msg: msg.to_string()};
    }
}

pub trait OptionExt<T> {
    fn or_err(self, msg: &str) -> std::result::Result<T, RihError>;
}

impl<T> OptionExt<T> for Option<T> {
    fn or_err(self, msg: &str) -> std::result::Result<T, RihError> {
        if let Some(x) = self {
            return Ok(x);
        }
        return Err(RihError::new(msg));
    }
}

impl std::fmt::Display for RihError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        return write!(f, "{}", self.msg);
    }
}

impl std::error::Error for RihError {
    fn description(&self) -> &str {
        return &self.msg;
    }

    fn cause(&self) -> Option<&std::error::Error> {
        return None;
    }
}
