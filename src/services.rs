use crate::bucket::BucketConfig;
use crate::errors::{process_error, S3PathOp};
use rusoto_core::RusotoError;
use rusoto_s3::{
    CommonPrefix, HeadObjectError, HeadObjectOutput, HeadObjectRequest, ListObjectsError,
    ListObjectsV2Request, Object, S3Client, S3,
};
use std::fmt::{Debug, Formatter};

pub struct S3Service {
    pub bucket: BucketConfig,
    pub client: S3Client,
}

impl Debug for S3Service {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Service")
            .field("bucket", &self.bucket)
            .finish()
    }
}

impl S3Service {
    #[tokio::main]
    pub async fn object_exists(&self) -> Result<HeadObjectOutput, RusotoError<HeadObjectError>> {
        let head_object_input = HeadObjectRequest {
            bucket: self.bucket.name.to_string(),
            expected_bucket_owner: None,
            if_match: None,
            if_modified_since: None,
            if_none_match: None,
            if_unmodified_since: None,
            key: self.bucket.key.to_string(),
            part_number: None,
            range: None,
            request_payer: None,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            version_id: None,
        };

        self.client.head_object(head_object_input).await
    }

    pub(crate) fn eager_loading_object_exists(&self) -> Result<bool, ()> {
        let does_object_exists = self.object_exists();

        match does_object_exists {
            Ok(_) => {}
            Err(e) => process_error(e, S3PathOp::HeadObject),
        }

        Ok(true)
    }

    #[tokio::main]
    pub async fn list_objects(
        &self,
    ) -> Result<(Vec<Object>, Vec<CommonPrefix>, String), RusotoError<ListObjectsError>> {
        let mut objects = vec![];
        let mut common_prefixes = vec![];
        let mut prefix = String::new();

        let mut list_object_input = ListObjectsV2Request {
            bucket: self.bucket.name.to_string(),
            continuation_token: None,
            delimiter: Option::Some("/".to_string()),
            encoding_type: None,
            expected_bucket_owner: None,
            fetch_owner: None,
            max_keys: None,
            prefix: self.bucket.full_path.clone(),
            request_payer: None,
            start_after: None,
        };

        loop {
            let result = self.client.list_objects_v2(list_object_input.clone()).await;

            match result {
                Ok(list_objects_output) => {
                    if let Some(contents) = list_objects_output.contents {
                        objects.extend(contents);
                    }

                    let bucket_prefix = list_objects_output.prefix.unwrap();
                    prefix.push_str(bucket_prefix.as_str().split_at(bucket_prefix.len() - 1).0);

                    if let Some(prefixes) = list_objects_output.common_prefixes {
                        common_prefixes.extend(prefixes);
                    }

                    if list_objects_output.next_continuation_token.is_none() {
                        break;
                    } else {
                        list_object_input.continuation_token =
                            list_objects_output.continuation_token;
                    }
                }
                Err(e) => process_error(e, S3PathOp::ListObjects),
            }
        }

        Ok((objects, common_prefixes, prefix))
    }
}
