use crate::bucket::BucketConfig;
use crate::file::File;
use crate::services::S3Service;
use rusoto_core::Region;
use rusoto_s3::S3Client;
use std::ffi::OsStr;
use std::fmt::{Debug, Formatter};
use std::path::{Ancestors, Path, PathBuf};

pub struct S3Path {
    /// A `PathBuf` object representing the path.
    path: PathBuf,
    /// A tree-like `File` object that represents the files in the path.
    file: Option<File>,
    /// If `true`, all the required calls to AWS are made on instantiating i.e when any of
    /// `new`, `from_s3_client` or `from_bucket` is called.
    eager_loading: bool,
    /// An `S3Service` object. You probably do not need this but it could come in handy if you need
    /// to make calls to s3 yourself.
    s3_service: S3Service,
}

impl Debug for S3Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Path")
            .field("path", &self.path)
            .field("file", &self.file)
            .field("eager_loading", &self.eager_loading)
            .finish()
    }
}

impl S3Path {
    /// Create a new S3Path from an absolute path.
    /// This will create a new rusoto S3 client first (see ) and use the client for making requests.
    /// # Examples
    /// ```
    ///   use s3_fs::s3::S3Path;
    ///   let s3_path = S3Path::new("/foo/bar.txt", false);
    ///
    ///```
    /// # Panics
    ///
    /// Panics if `path` is not absolute.
    pub fn new<P: ToString + Copy>(path: P, eager_loading: bool) -> Self {
        let client = S3Client::new(Region::default());

        let bucket = BucketConfig::from_path(path);

        let mut file = Option::None;

        let s3_service = S3Service { bucket, client };

        if eager_loading && s3_service.eager_loading_object_exists().is_ok() {
            file = Self::build_files(&s3_service)
        }

        let path = Self::clean_path(path);

        S3Path {
            path,
            file,
            eager_loading,
            s3_service,
        }
    }

    /// Create an S3Path from an S3 client and use the client for making requests.
    /// # Examples
    /// ```no_run
    ///
    ///   use rusoto_s3::S3Client;
    ///   use rusoto_core::region::Region;
    ///   use s3_fs::s3::S3Path;
    ///   let s3_client = S3Client::new(Region::UsEast1);
    ///   let s3_path = S3Path::from_s3_client(s3_client, "/foo/bar.txt", false);
    ///
    ///```
    pub fn from_s3_client<P: ToString + Copy>(
        s3_client: S3Client,
        path: P,
        eager_loading: bool,
    ) -> Self {
        let bucket = BucketConfig::from_path(path);

        let mut file = Option::None;

        let s3_service = S3Service {
            bucket,
            client: s3_client,
        };

        if eager_loading {
            file = Self::build_files(&s3_service);
        }

        let path = PathBuf::from(path.to_string());

        Self::validate_path(&path);

        S3Path {
            path,
            file,
            eager_loading,
            s3_service,
        }
    }

    /// Create a new S3Path from a `BucketConfig`.
    /// This will create a new rusoto S3 client first (see ) and use the client for making requests.
    /// # Examples
    /// ```no_run
    ///
    ///   use s3_fs::s3::S3Path;
    ///   use s3_fs::bucket::BucketConfig;
    ///
    ///   let bucket = BucketConfig{name: "foo".to_string(), key: "bar".to_string(), full_path: None};
    ///   let s3_path = S3Path::from_bucket(bucket, false);
    ///
    ///```
    pub fn from_bucket(bucket: BucketConfig, lazy_loading: bool) -> Self {
        let path = format!("/{}/{}", bucket.name, bucket.key);

        Self::new(&path, lazy_loading)
    }

    /// Returns `true` if the `S3Path` is absolute.
    /// # Examples
    /// ```
    ///
    ///   use s3_fs::s3::S3Path;
    ///
    ///   let s3_path = S3Path::new("/foo/bar", false);
    ///   assert_eq!(s3_path.is_absolute(), true);
    ///
    ///```
    ///
    pub fn is_absolute(&self) -> bool {
        self.path.is_absolute()
    }

    /// Returns `true` if the `S3Path` is relative.
    /// # Examples
    /// ```
    ///
    ///   use s3_fs::s3::S3Path;
    ///
    ///   let s3_path = S3Path::new("foo/bar", false);
    ///   assert_eq!(s3_path.is_relative(), true);
    ///
    ///```
    ///
    pub fn is_relative(&self) -> bool {
        self.path.is_relative()
    }

    /// Always returns `true` if `eager_loading` is enabled. If not, it makes a call to AWS to check.
    /// # Examples
    ///```
    /// use s3_fs::s3::S3Path;
    /// let s3_path = S3Path::new("/foo/bar", false);
    /// assert_eq!(s3_path.exists(), false)
    ///
    /// ```
    pub fn exists(&self) -> bool {
        match self.eager_loading {
            true => true,
            false => self.s3_service.object_exists().is_ok(),
        }
    }

    /// Returns `true` if the `S3Path` is a directory
    /// # Examples
    /// ```
    ///
    ///   use s3_fs::s3::S3Path;
    ///   let s3_path = S3Path::new("/foo/bar", false);
    ///   assert_eq!(s3_path.is_dir(), false);
    ///
    ///```
    pub fn is_dir(&self) -> bool {
        match self.eager_loading {
            true => self.file.as_ref().unwrap().is_dir(),
            false => match self.exists() {
                true => {
                    let file = Self::build_files(&self.s3_service);
                    file.as_ref().unwrap().is_dir()
                }
                false => false,
            },
        }
    }

    /// Returns `true` if the `S3Path` is a file
    /// # Examples
    /// ```
    ///
    ///   use s3_fs::s3::S3Path;
    ///   let s3_path = S3Path::new("/foo/bar", false);
    ///   assert_eq!(s3_path.is_file(), false);
    ///
    ///```
    pub fn is_file(&self) -> bool {
        !self.is_dir()
    }

    /// Returns the name of file.
    /// If the path is a normal file, it returns the file name. If it's the path of a directory,
    /// it returns the directory name.
    pub fn file_name(&self) -> Option<&OsStr> {
        self.path.file_name()
    }

    pub fn ancestors(&self) -> Ancestors<'_> {
        self.path.ancestors()
    }

    pub fn extension(&self) -> Option<&OsStr> {
        self.path.extension()
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

    fn build_files(s3_service: &S3Service) -> Option<File> {
        let mut file = Option::None;

        if let Ok((objects, common_prefixes, prefix)) = s3_service.list_objects() {
            file = Option::Some(File::new(
                &s3_service.bucket.key,
                objects,
                prefix,
                common_prefixes,
            ));
        }

        file
    }
}
