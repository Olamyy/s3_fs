use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq)]
pub struct BucketConfig {
    /// The name of the bucket.
    pub name: String,
    /// The bucket key
    pub key: String,
    /// A path
    pub full_path: Option<String>,
}

impl BucketConfig {
    /// Create a new bucket configuration from a path. This can either be
    /// a full s3 path or a shortened path
    /// ```
    ///
    ///   use s3_fs::bucket::BucketConfig;
    ///   let bucket_config = BucketConfig::from_path("s3://bucket/key");
    ///   assert_eq!(bucket_config, BucketConfig{name: "bucket".to_string(), key: "key/".to_string(), full_path: Option::from("key/".to_string())})
    ///
    ///```
    ///
    ///  #Panics
    ///
    ///  Panics if the bucket name is not valid.
    pub fn from_path<P: ToString>(path: P) -> Self {
        let (name, key, full_path) = Self::split_path(path.to_string());
        BucketConfig {
            name,
            key,
            full_path,
        }
    }

    fn split_path(path: String) -> (String, String, Option<String>) {
        let path = path
            .replace("s3://", "")
            .replace(":accesspoint/", ":accesspoint:");
        let parts = path
            .split('/')
            .filter(|n| !n.is_empty())
            .collect::<Vec<&str>>();

        let bucket = parts[0];
        if bucket.contains('/') {
            panic!("{} is not a valid bucket name.", bucket)
        }

        let mut key = String::new();
        let mut full_path = Option::None;

        match parts.len().cmp(&2) {
            Ordering::Less => {}
            Ordering::Equal => {
                let other = format!("{}/", parts[1]);
                key.push_str(&other);
                full_path = Option::Some(other);
            }
            Ordering::Greater => {
                key.push_str(&format!("{}/", parts[1]));
                full_path = Option::Some(format!("{}/", parts[1..].join("/")));
            }
        }

        (bucket.to_string(), key.to_string(), full_path)
    }
}
