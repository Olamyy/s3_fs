use s3_fs::s3::S3Path;

fn main() {
    let s3_path = S3Path::new(
        "s3://ola-testing-model-registry/dir/data_source/inner_inner/support_image.png",
        true,
    );
    dbg!(s3_path.is_dir());
    dbg!(s3_path.is_file());
    dbg!(s3_path.extension());
    for ancestor in s3_path.ancestors() {
        println!("Ancestor : {:?}", ancestor.extension())
    }
}
