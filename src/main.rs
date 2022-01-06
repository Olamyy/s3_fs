use s3_fs::fs;

fn main() {
    fs::create_dir_all("/foo/bar");
}
