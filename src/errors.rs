use rusoto_core::RusotoError;

#[derive(Debug)]
pub enum S3PathError {
    Unknown,
    ExpiredToken,
    ObjectDoesNotExist,
}

#[derive(Debug)]
pub enum S3PathOp {
    HeadObject,
    ListObjects,
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

pub fn process_error<E>(e: RusotoError<E>, op: S3PathOp) {
    match e {
        RusotoError::Unknown(e) => match e.status.as_str() {
            "400" => panic!("{}", S3PathError::ExpiredToken),
            "404" => {
                if let S3PathOp::HeadObject = op {
                    panic!("{}", S3PathError::ObjectDoesNotExist)
                }
            }
            _ => panic!("{}", S3PathError::Unknown),
        },
        _ => panic!("Something else happened"),
    }
}
