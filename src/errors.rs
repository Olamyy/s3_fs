use rusoto_core::RusotoError;
use std::fmt::Debug;

#[derive(Debug)]
pub enum S3PathError {
    Unknown,
    ExpiredToken,
    ObjectDoesNotExist,
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub enum S3PathOp {
    HeadObject,
    GetObject,
    PutObject,
}

impl std::error::Error for S3PathError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            S3PathError::Unknown => None,
            S3PathError::ExpiredToken => None,
            S3PathError::ObjectDoesNotExist => None,
        }
    }
}

impl std::fmt::Display for S3PathError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            S3PathError::Unknown => {
                write!(f, "Something unexpected happened.")
            }
            S3PathError::ExpiredToken => {
                write!(f, "The provided token has expired.")
            }
            S3PathError::ObjectDoesNotExist => {
                write!(f, "No such file or directory.")
            }
        }
    }
}

pub fn process_error<E: Debug>(e: RusotoError<E>, op: S3PathOp) {
    match e {
        RusotoError::Unknown(e) => match e.status.as_str() {
            "400" => panic!("{}", S3PathError::ExpiredToken),
            "404" | "301" => {
                if let S3PathOp::HeadObject = op {
                    panic!("{}", S3PathError::ObjectDoesNotExist)
                }
            }
            _ => panic!("{} : {:?}", S3PathError::Unknown, e),
        },
        _ => panic!("{:?}", e),
    }
}
