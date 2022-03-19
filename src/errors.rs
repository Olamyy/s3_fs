use rusoto_core::RusotoError;
use std::fmt::Debug;

#[derive(Debug, PartialEq)]
pub enum S3PathError {
    Unknown,
    ExpiredToken,
    ObjectDoesNotExist,
    ObjectAlreadyExists,
    NotADirectory,
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub enum S3PathOp {
    HeadObject,
    GetObject,
    PutObject,
    ListObjects,
}

impl std::error::Error for S3PathError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            S3PathError::Unknown => None,
            S3PathError::ExpiredToken => None,
            S3PathError::ObjectDoesNotExist => None,
            S3PathError::ObjectAlreadyExists => None,
            S3PathError::NotADirectory => None,
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
            S3PathError::ObjectAlreadyExists => {
                write!(f, "The file/folder already exists.")
            }
            S3PathError::NotADirectory => {
                write!(f, "The provided path is not a directory")
            }
        }
    }
}

pub fn process_error<E: Debug>(
    e: Option<RusotoError<E>>,
    s3_path_error: Option<S3PathError>,
    op: S3PathOp,
) -> S3PathError {
    match e {
        None => s3_path_error.unwrap(),
        Some(rusoto_error) => match rusoto_error {
            RusotoError::Service(_) => S3PathError::Unknown,
            RusotoError::Unknown(error) => match error.status.as_str() {
                "400" => S3PathError::ExpiredToken,
                "404" | "301" => {
                    if let S3PathOp::HeadObject = op {
                        S3PathError::ObjectDoesNotExist
                    } else {
                        S3PathError::Unknown
                    }
                }
                _ => S3PathError::Unknown,
            },
            _ => S3PathError::Unknown,
        },
    }
}
