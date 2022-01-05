use crate::s3::S3Path;
use crate::services::S3Service;

struct FS {
    pub path: S3Path,
    service: S3Service,
}

impl FS {
    pub fn new(path: S3Path) -> Self {
        Self::ensure_paths_exists(&path).unwrap();

        let service = S3Service::new(path.path.to_str().unwrap().to_string());

        FS { path, service }
    }

    pub fn from_string<P>(path: P) -> FS
    where
        P: ToString + Copy,
    {
        let path = S3Path::new(path);
        let service = S3Service::new(path.path.to_str().unwrap().to_string());

        FS { path, service }
    }

    pub fn copy<P>(&self, to: P) -> Result<Option<i64>, ()>
    where
        P: ToString + Copy,
    {
        let from_content = self.service.get_object_body().unwrap();

        let from_metadata = self.path.metadata().unwrap();

        match self.service.write_to_object(
            from_metadata.content_length,
            from_content,
            to,
            self.path.metadata().unwrap().metadata,
        ) {
            Ok(_) => Ok(from_metadata.content_length),
            Err(_) => Err(()),
        }
    }

    pub fn create_dir(&self, path: &S3Path) -> Result<String, ()> {
        let dir_name = path.path.to_str().unwrap();
        match self
            .service
            .write_to_object(None, None, self.service.bucket.key.to_string(), None)
        {
            Ok(_) => Ok(dir_name.to_string()),
            Err(_) => Err(()),
        }
    }

    fn ensure_paths_exists(path: &S3Path) -> Result<bool, ()> {
        path.try_exists()
    }
}

/// Copies the contents of one S3 object to another. This function will overwrite the contents of `to`.
/// On success, it returns the content_length of the object
///
/// # Example
///
/// ```no_run
/// use s3_fs::fs;
/// use s3_fs::s3::S3Path;
/// let s3_path = S3Path::new("foo/from.txt");
///     fs::copy(
///         s3_path,
///         "foo/to.txt",
///     );
///
///let copied_path = S3Path::new("foo/to.txt");
/// copied_path.try_exists();
/// ```
///
/// # Panics
///
/// Panics if anything goes wrong when making the PutObject call.
#[allow(clippy::result_unit_err)]
pub fn copy<P>(from: S3Path, to: P) -> Result<Option<i64>, ()>
where
    P: ToString + Copy,
{
    let fs = FS::new(from);

    fs.copy(to)
}

/// Creates a new directory in an s3 bucket.
///
///
/// # Note
/// If you do not pass a full S3 path, the function splits the provided path with "/" and assumes the first part is the bucket.
/// i.e The bucket name in "foo/bar/doo/dah" is "foo".
///
/// # Example
///
/// ```no_run
/// use s3_fs::fs;
/// use s3_fs::s3::S3Path;
/// fs::create_dir(
///         "foo/some_dir",
///     );
///
/// S3Path::new("foo/some_dir").try_exists();
/// ```
///
/// # Panics
///
/// Panics if anything goes wrong when making the PutObject call.
#[allow(clippy::result_unit_err)]
pub fn create_dir<P>(path: P) -> Result<String, ()>
where
    P: ToString + Copy,
{
    let fs = FS::from_string(path);

    fs.create_dir(&fs.path)
}
