use crate::bucket::BucketConfig;
use crate::errors::{process_error, S3PathError, S3PathOp};
use crate::object::{ObjectMetadata, S3ObjectType};
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{
    CommonPrefix, GetObjectError, GetObjectOutput, GetObjectRequest, HeadObjectError,
    HeadObjectOutput, HeadObjectRequest, ListObjectsError, ListObjectsV2Error, ListObjectsV2Output,
    ListObjectsV2Request, Object, PutObjectError, PutObjectOutput, PutObjectRequest, S3Client,
    StreamingBody, S3,
};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::hash::Hasher;
use crate::fs::metadata;

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
    pub fn new(path: String) -> Self {
        let client = S3Client::new(Region::default());
        let bucket = BucketConfig::from_path(path);
        S3Service { bucket, client }
    }

    pub fn from_client(path: String, client: S3Client) -> Self {
        let bucket = BucketConfig::from_path(path);
        S3Service { bucket, client }
    }

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

    #[tokio::main]
    async fn get_object(&self) -> Result<GetObjectOutput, RusotoError<GetObjectError>> {
        let get_object_input = GetObjectRequest {
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
            response_cache_control: None,
            response_content_disposition: None,
            response_content_encoding: None,
            response_content_language: None,
            response_content_type: None,
            response_expires: None,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            version_id: None,
        };

        self.client.get_object(get_object_input).await
    }

    #[tokio::main]
    async fn put_object<P: ToString>(
        &self,
        content_length: Option<i64>,
        body: Option<StreamingBody>,
        path: P,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<PutObjectOutput, RusotoError<PutObjectError>> {
        let put_object_request = PutObjectRequest {
            acl: None,
            body,
            bucket: self.bucket.name.to_string(),
            bucket_key_enabled: None,
            cache_control: None,
            content_disposition: None,
            content_encoding: None,
            content_language: None,
            content_length,
            content_md5: None,
            content_type: None,
            expected_bucket_owner: None,
            expires: None,
            grant_full_control: None,
            grant_read: None,
            grant_read_acp: None,
            grant_write_acp: None,
            key: path.to_string(),
            metadata,
            object_lock_legal_hold_status: None,
            object_lock_mode: None,
            object_lock_retain_until_date: None,
            request_payer: None,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            ssekms_encryption_context: None,
            ssekms_key_id: None,
            server_side_encryption: None,
            storage_class: None,
            tagging: None,
            website_redirect_location: None,
        };

        self.client.put_object(put_object_request).await
    }

    pub fn write_to_object<P: ToString>(
        &self,
        content_length: Option<i64>,
        body: Option<StreamingBody>,
        path: P,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<PutObjectOutput, S3PathError> {
        match self.put_object(content_length, body, path, metadata) {
            Ok(result) => Ok(result),
            Err(e) => Err(process_error(Some(e), None, S3PathOp::PutObject)),
        }
    }

    pub fn ensure_object_exists(&self) -> Result<bool, S3PathError> {
        match self.object_exists() {
            Ok(_) => Ok(true),
            Err(e) => Err(process_error(Some(e), None, S3PathOp::HeadObject)),
        }
    }

    pub fn get_object_body(&self) -> Result<Option<StreamingBody>, S3PathError> {
        match self.get_object() {
            Ok(body) => Ok(body.body),
            Err(e) => Err(process_error(Some(e), None, S3PathOp::GetObject)),
        }
    }

    pub fn get_object_metadata(&self) -> Result<ObjectMetadata, S3PathError> {
        match self.get_object() {
            Ok(object) => {
                let file_type = match self.bucket.key.contains(".") {
                    true => S3ObjectType::File,
                    false => S3ObjectType::Directory,
                };
                let metadata = ObjectMetadata {
                    content_type: object.content_type.unwrap(),
                    content_length: object.content_length,
                    e_tag: object.e_tag.unwrap(),
                    last_modified: object.last_modified.unwrap(),
                    metadata: object.metadata,
                    object_type: file_type
                };

                Ok(metadata)
            },
            Err(e) => Err(process_error(Some(e), None, S3PathOp::GetObject)),
        }
    }

    #[tokio::main]
    pub async fn list_objects(
        &self,
    ) -> Result<(Vec<Object>, Vec<CommonPrefix>, String), S3PathError> {
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
            prefix: Some(self.bucket.key.to_string()),
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
                Err(_) => {},
            }
        }

        Ok((objects, common_prefixes, prefix))
    }
}
