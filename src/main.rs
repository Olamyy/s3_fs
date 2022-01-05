use s3_fs::fs;
use s3_fs::s3::S3Path;

fn main() {
    let s3_path = S3Path::new("/ola-testing-model-registry/dir/data_source/inner_inner/test.txt");
    fs::copy(s3_path, "dir/data_source/inner_inner/new_copy.txt").unwrap();

    let copied_path =
        S3Path::new("/ola-testing-model-registry/dir/data_source/inner_inner/new_copy.txt");
    dbg!(&copied_path.exists());

    // fs::create_dir("/ola-testing-model-registry/dir/data_source/inner_inner/ICreatedYou").unwrap();
}
