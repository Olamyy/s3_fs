use s3_fs::dir::DirEntry;
use s3_fs::errors::S3PathError;
use s3_fs::fs;
use s3_fs::s3::S3Path;


fn main() -> Result<(), S3PathError> {
    // let path_1 = S3Path::new("s3://ola-testing-model-registry/folder/inner/test.txt");
    // let path_2 = S3Path::new("s3://ola-testing-model-registry/folder/inner/test.txt");
    // let path_3 = S3Path::new("s3://ola-testing-model-registry/folder/inner/test.txt");
    //
    // let children = vec![path_1, path_2, path_3];
    //
    // let mut dirs = DirEntry { items: children };
    //
    // for dir in dirs {
    //     dbg!(&dir);
    // }

    dbg!(fs::read_dir("s3://ola-testing-model-registry/folder/").err());

    Ok(())
}
