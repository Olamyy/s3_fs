use crate::errors::S3PathError;
use crate::object::ObjectMetadata;
use crate::s3::S3Path;
use std::io::Read;
use rusoto_s3::Object;

#[derive(Debug)]
struct FS {
    pub path: S3Path,
}

impl FS {
    pub fn new(path: S3Path) -> Self {
        Self::ensure_paths_exists(&path).unwrap();

        FS { path }
    }

    pub fn from_string<P>(path: P) -> FS
    where
        P: ToString + Copy,
    {
        let path = S3Path::new(path);

        FS { path }
    }

    pub fn copy<P>(&self, to: P) -> Result<Option<i64>, S3PathError>
    where
        P: ToString + Copy,
    {
        let from_content = self.path.service.get_object_body()?;

        let from_metadata = self.path.metadata()?;

        self.path.service.write_to_object(
            from_metadata.content_length,
            from_content,
            to,
            self.path.metadata().unwrap().metadata,
        )?;

        Ok(from_metadata.content_length)
    }

    pub fn create_dir(&self) -> Result<String, S3PathError> {
        self.path.service.write_to_object(
            None,
            None,
            self.path.service.bucket.key.to_string(),
            None,
        )?;

        Ok(self.path.to_string())
    }

    pub fn metadata(&self) -> Result<ObjectMetadata, S3PathError> {
        self.path.service.get_object_metadata()
    }

    pub fn read(&self) -> Result<Vec<u8>, S3PathError> {
        let body = self.path.service.get_object_body()?;

        let mut stream = body.unwrap().into_blocking_read();

        let mut body = Vec::new();

        stream.read_to_end(&mut body).unwrap();

        Ok(body)
    }

    pub fn read_dir(&self) -> Result<(), S3PathError> {
        self.path.try_exists()?;

        if !self.path.is_dir() {
            return Err(S3PathError::NotADirectory);
        }

        let (objects, prefix, common_prefixes) = self.path.service.list_objects()?;

        let path = self.path.to_string();
        let dir_paths = path
            .split("/")
            .filter(|path| !path.is_empty())
            .collect::<Vec<_>>();
        let dir_name = dir_paths.last().unwrap();

        let mut valid_s3_objects = objects.into_iter()
            .filter(|object| object.key.is_some() && object.key != Some(dir_name.to_string()))
            .collect::<Vec<Object>>();

        dbg!(valid_s3_objects);

        Ok(())
    }

    fn ensure_paths_exists(path: &S3Path) -> Result<bool, S3PathError> {
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
pub fn copy<P>(from: S3Path, to: P) -> Result<Option<i64>, S3PathError>
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
/// 1. If the parent does not exist.
/// 1. If anything goes wrong when making the PutObject call.
pub fn create_dir<P>(path: P) -> Result<String, S3PathError>
where
    P: ToString + Copy,
{
    let path = path.to_string();

    let parent = path.split('/').collect::<Vec<_>>();
    let parent_path = &parent[0..parent.len() - 2].join("/");

    let parent_fs = FS::from_string(parent_path.as_str());
    parent_fs.path.service.ensure_object_exists()?;

    let child_fs = FS::from_string(path.as_str());

    child_fs.create_dir()
}

/// Recursively create a directory and all of its parent components if they are missing.
/// All [`s3_fs::fs::create_dir`] apply.
///
/// # Example
///
/// ```no_run
/// use s3_fs::fs;
/// use s3_fs::s3::S3Path;
/// fs::create_dir_all(
///         "foo/some_dir/bar/",
///     );
///
/// S3Path::new("foo/some_dir/").try_exists();
/// S3Path::new("foo/some/bar/").try_exists();
/// ```
///
/// # Panics
///
/// Panics if anything goes wrong when making the PutObject call.
pub fn create_dir_all<P>(path: P) -> Result<String, S3PathError>
where
    P: ToString + Copy,
{
    let fs = FS::from_string(path);

    fs.create_dir()
}

/// Given a path in a bucket, get information about the file or directory it points to.
/// All [`s3_fs::fs::create_dir`] apply.
///
/// # Example
///
/// ```no_run
/// use s3_fs::fs;
/// use s3_fs::s3::S3Path;
/// fs::metadata(
///         "foo/some_dir/bar/",
///     );
///
/// ```
///
/// # Panics
///
/// Panics if anything goes wrong when making the call to AWS.

pub fn metadata<P>(path: P) -> Result<ObjectMetadata, S3PathError>
where
    P: ToString + Copy,
{
    let fs = FS::from_string(path);

    fs.metadata()
}

pub fn read<P>(path: P) -> Result<Vec<u8>, S3PathError>
where
    P: ToString + Copy,
{
    let fs = FS::from_string(path);

    fs.read()
}

pub fn read_dir<P>(path: P) -> Result<(), S3PathError>
where
    P: ToString + Copy,
{
    let fs = FS::from_string(path);

    fs.read_dir()
}
