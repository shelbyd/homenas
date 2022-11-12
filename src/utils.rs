use crate::io::*;

pub trait ResultExt<T> {
    fn into_found(self) -> Result<Option<T>, IoError>;
}

impl<T> ResultExt<T> for Result<T, IoError> {
    fn into_found(self) -> Result<Option<T>, IoError> {
        match self {
            Ok(t) => Ok(Some(t)),
            Err(IoError::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }
}
