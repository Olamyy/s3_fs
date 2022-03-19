use crate::s3::S3Path;

#[derive(Debug)]
pub struct DirEntry {
    pub items: Vec<S3Path>,
}

impl Iterator for DirEntry {
    type Item = S3Path;

    fn next(&mut self) -> Option<Self::Item> {
        match self.items.get(0) {
            None => None,
            Some(_) => {
                let result = self.items.remove(0);
                Some(result)
            }
        }
    }
}
