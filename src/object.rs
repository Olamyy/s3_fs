use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use crate::errors::S3PathError;

#[derive(Clone, Debug, PartialEq)]
pub enum S3ObjectType {
    File,
    Directory,
}

pub struct ObjectMetadata {
    pub content_type: String,
    pub content_length: Option<i64>,
    pub e_tag: String,
    pub last_modified: String,
    pub metadata: Option<HashMap<String, String>>,
    pub object_type: S3ObjectType
}

impl Debug for ObjectMetadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Object")
            .field("content_type", &self.content_type)
            .field("content_length", &self.content_length)
            .field("e_tag", &self.e_tag)
            .field("last_modified", &self.last_modified)
            .field("metadata", &self.metadata)
            .finish()
    }
}
