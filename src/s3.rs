use crate::bucket::BucketConfig;
use crate::errors::S3PathError;
use crate::object::{ObjectMetadata, S3ObjectType};
use crate::services::S3Service;
use rusoto_s3::S3Client;
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};

pub struct S3Path {
    /// A `PathBuf` object representing the path.
    pub path: PathBuf,
    /// A [ObjectContent] representation of the content of the path.
    pub service: S3Service,
}

impl Debug for S3Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path.display())
    }
}

impl S3Path {
    /// Create a new S3Path from an absolute path.
    /// This will create a new rusoto S3 client first (see ) and use the client for making requests.
    /// # Examples
    /// ```
    ///   use s3_fs::s3::S3Path;
    ///   let s3_path = S3Path::new("/foo/bar.txt");
    ///
    ///```
    /// # Panics
    ///
    /// Panics if `path` is not absolute.
    pub fn new<P: ToString + Copy>(path: P) -> Self {
        let service = S3Service::new(path.to_string());
        let path = Self::clean_path(path);

        S3Path { path, service }
    }

    /// Create an S3Path from an S3 client and use the client for making requests.
    /// # Examples
    /// ```no_run
    ///
    ///   use rusoto_s3::S3Client;
    ///   use rusoto_core::region::Region;
    ///   use s3_fs::s3::S3Path;
    ///   let s3_client = S3Client::new(Region::UsEast1);
    ///   let s3_path = S3Path::from_s3_client("/foo/bar.txt", s3_client);
    ///
    ///```
    pub fn from_s3_client<P: ToString + Copy>(path: P, s3_client: S3Client) -> Self {
        let service = S3Service::from_client(path.to_string(), s3_client);

        let path = PathBuf::from(path.to_string());

        Self::validate_path(&path);

        S3Path { path, service }
    }

    /// Create a new S3Path from a `BucketConfig`.
    /// This will create a new rusoto S3 client first (see ) and use the client for making requests.
    /// # Examples
    /// ```no_run
    ///
    ///   use s3_fs::s3::S3Path;
    ///   use s3_fs::bucket::BucketConfig;
    ///
    ///   let bucket = BucketConfig{name: "foo".to_string(), key: "bar".to_string()};
    ///   let s3_path = S3Path::from_bucket(bucket);
    ///
    ///```
    pub fn from_bucket(bucket: BucketConfig) -> Self {
        let path = format!("/{}/{}", bucket.name, bucket.key);

        Self::new(&path)
    }

    /// Returns `true` if the object exists
    /// # Examples
    ///```
    /// use s3_fs::s3::S3Path;
    /// let s3_path = S3Path::new("/foo/bar");
    /// assert_eq!(s3_path.exists(), false)
    ///
    /// ```
    pub fn exists(&self) -> bool {
        self.service.object_exists().is_ok()
    }

    /// Returns `true` if the object exists
    /// # Examples
    ///```
    /// use s3_fs::s3::S3Path;
    /// let s3_path = S3Path::new("/foo/bar");
    /// assert_eq!(s3_path.exists(), false)
    ///
    /// ```
    /// # Panics
    ///
    /// Panics if the object does not exist
    ///
    ///
    pub fn try_exists(&self) -> Result<bool, S3PathError> {
        self.service.ensure_object_exists()
    }

    /// Returns `true` if the `S3Path` is a directory
    /// # Examples
    /// ```
    ///
    ///   use s3_fs::s3::S3Path;
    ///   let s3_path = S3Path::new("/foo/bar/");
    ///   s3_path.is_dir();
    ///
    ///```
    pub fn is_dir(&self) -> bool {
        match self.exists() {
            true => {
                let metadata = self.service.get_object_metadata().unwrap();
                metadata.object_type == S3ObjectType::Directory
            }
            false => false,
        }
    }

    /// Returns `true` if the `S3Path` is a file
    /// # Examples
    /// ```
    ///
    ///   use s3_fs::s3::S3Path;
    ///   let s3_path = S3Path::new("/foo/bar");
    ///   s3_path.is_file();
    ///
    ///```
    pub fn is_file(&self) -> bool {
        !self.is_dir()
    }

    /// Returns `true` if the `S3Path` is a file
    /// # Examples
    /// ```
    ///
    ///   use s3_fs::s3::S3Path;
    ///   let s3_path = S3Path::new("/foo/bar");
    ///   s3_path.metadata();
    ///
    ///```
    pub fn metadata(&self) -> Result<ObjectMetadata, S3PathError> {
        self.service.get_object_metadata()
    }

    fn validate_path(path: &Path) {
        if !path.starts_with("s3://") && path.is_relative() {
            panic!("Found a relative path. S3Path only works with absolute paths.")
        }
    }

    fn clean_path<P: ToString + Copy>(path: P) -> PathBuf {
        let path = path.to_string();
        if path.starts_with("s3://") {
            PathBuf::from(
                path.splitn(4, '/')
                    .collect::<Vec<&str>>()
                    .last()
                    .unwrap()
                    .to_string(),
            )
        } else {
            let path = PathBuf::from(path);
            if path.is_relative() {
                panic!("Found a relative path. S3Path only works with absolute paths.")
            }
            path
        }
    }
}

impl ToString for S3Path {
    fn to_string(&self) -> String {
        self.path.to_str().unwrap().to_string()
    }
}
