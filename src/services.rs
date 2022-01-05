use crate::bucket::BucketConfig;
use crate::errors::{process_error, S3PathOp};
use crate::object::ObjectMetadata;
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{
    GetObjectError, GetObjectOutput, GetObjectRequest, HeadObjectError, HeadObjectOutput,
    HeadObjectRequest, PutObjectError, PutObjectOutput, PutObjectRequest, S3Client, StreamingBody,
    S3,
};
use std::collections::HashMap;
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
    ) -> Result<PutObjectOutput, ()> {
        let result = self.put_object(content_length, body, path, metadata);
        match result {
            Ok(put_object_result) => Ok(put_object_result),
            Err(e) => {
                process_error(e, S3PathOp::PutObject);
                Err(())
            }
        }
    }

    pub fn ensure_object_exists(&self) -> Result<bool, ()> {
        let does_object_exists = self.object_exists();

        match does_object_exists {
            Ok(_) => {}
            Err(e) => process_error(e, S3PathOp::HeadObject),
        }

        Ok(true)
    }

    pub fn get_object_body(&self) -> Result<Option<StreamingBody>, ()> {
        let s3_object = self.get_object();

        match s3_object {
            Ok(object) => Ok(object.body),
            Err(e) => {
                process_error(e, S3PathOp::GetObject);
                Err(())
            }
        }
    }

    pub fn get_object_metadata(&self) -> Result<ObjectMetadata, ()> {
        let s3_object = self.get_object();

        match s3_object {
            Ok(object) => Ok(ObjectMetadata {
                content_type: object.content_type.unwrap(),
                content_length: object.content_length,
                e_tag: object.e_tag.unwrap(),
                last_modified: object.last_modified.unwrap(),
                metadata: object.metadata,
            }),
            Err(e) => {
                process_error(e, S3PathOp::GetObject);
                Err(())
            }
        }
    }
}
