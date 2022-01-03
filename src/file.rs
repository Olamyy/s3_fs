use rusoto_s3::{CommonPrefix, Object};

#[derive(Clone, Debug, PartialEq)]
pub enum S3ObjectType {
    File,
    Directory,
}

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub last_modified: Option<String>,
    pub size: Option<i64>,
    pub extension: Option<String>,
    pub file_type: S3ObjectType,
}

impl FileMetadata {
    fn new(name: String, s3_objects: Vec<&Object>) -> Self {
        let mut metadata = FileMetadata {
            last_modified: None,
            size: None,
            extension: None,
            file_type: S3ObjectType::Directory,
        };

        if name.contains('.') {
            let maybe_extension = name.split('.').collect::<Vec<&str>>();
            metadata.extension = Some(maybe_extension.last().unwrap().to_string());
            metadata.file_type = S3ObjectType::File;
        }

        if !s3_objects.is_empty() {
            let object = s3_objects[0];
            metadata.size = object.clone().size;
            metadata.last_modified = object.clone().last_modified;
        }
        metadata
    }
}

#[derive(Debug, Clone)]
pub struct File {
    path: String,
    name: String,
    parent: Option<String>,
    children: Vec<File>,
    prefix: String,
    pub metadata: FileMetadata,
}

impl File {
    pub fn new(
        name: &str,
        objects: Vec<Object>,
        prefix: String,
        common_prefixes: Vec<CommonPrefix>,
    ) -> Self {
        let valid_s3_objects = objects
            .into_iter()
            .filter(|object| object.key.is_some() && object.key != Some(name.to_string()))
            .collect::<Vec<_>>();

        let longest_path = Self::longest_path(name, &valid_s3_objects, &prefix, &common_prefixes);

        let mut root_directory = Self::build_file(
            prefix.clone(),
            Self::name_without_slash(name),
            None,
            valid_s3_objects.clone(),
        );

        let mut children: Vec<File> = vec![];

        let mut first_child = Self::build_file(
            prefix.clone(),
            longest_path[0].to_string(),
            Some(root_directory.clone().path),
            valid_s3_objects.clone(),
        );

        let other_paths = &longest_path[1..];
        let other_path_iter = other_paths.iter();

        for (index, _) in other_path_iter.enumerate() {
            let current_path_name = other_paths.get(index).unwrap().to_string();

            let mut current_child = Self::build_file(
                prefix.clone(),
                current_path_name,
                Some(first_child.clone().path),
                valid_s3_objects.clone(),
            );

            let next: Option<String> = other_paths
                .get(index + 1)
                .and_then(|value| value.parse().ok());

            match next {
                None => first_child.add_child(current_child),
                Some(value) => {
                    let next_child = Self::build_file(
                        prefix.clone(),
                        value,
                        Some(current_child.clone().path),
                        valid_s3_objects.clone(),
                    );
                    current_child.add_child(next_child);
                    first_child.add_child(current_child);
                }
            }
        }

        children.push(first_child);

        root_directory.add_children(children);

        root_directory
    }

    pub fn is_dir(&self) -> bool {
        self.leaf_child().unwrap().metadata.file_type == S3ObjectType::Directory
    }

    pub fn _metadata(&self) -> FileMetadata {
        self.leaf_child().unwrap().metadata.to_owned()
    }

    fn longest_path<'a>(
        name: &'a str,
        objects: &'a [Object],
        prefix: &'a String,
        common_prefixes: &'a [CommonPrefix],
    ) -> Vec<&'a str> {
        let mut all_paths = common_prefixes
            .iter()
            .map(|prefix| prefix.prefix.as_ref())
            .collect::<Vec<_>>();

        all_paths.push(Some(prefix));

        let s3_object_paths = objects
            .iter()
            .map(|object| object.key.as_ref())
            .collect::<Vec<_>>();

        all_paths.extend(s3_object_paths);

        all_paths
            .iter()
            .map(|path| {
                path.unwrap()
                    .split('/')
                    .filter(|o| !o.is_empty() && *o != Self::name_without_slash(name))
                    .collect::<Vec<&str>>()
            })
            .max()
            .unwrap()
    }

    fn build_file(
        prefix: String,
        file_name: String,
        parent: Option<String>,
        s3_objects: Vec<Object>,
    ) -> File {
        let valid_s3_objects = Self::filter_objects_for_path(&file_name, &s3_objects);
        let file_id = Self::generate_file_id(&file_name, &parent);

        File {
            path: file_id,
            name: file_name.clone(),
            parent,
            children: vec![],
            prefix,
            metadata: FileMetadata::new(file_name.clone(), valid_s3_objects),
        }
    }

    fn generate_file_id(file_name: &str, parent: &Option<String>) -> String {
        let mut file_id = String::new();

        if parent.is_some() {
            let id = format!("{}/{}", parent.clone().unwrap(), file_name);
            file_id.push_str(&id);
        } else {
            file_id.push_str(file_name);
        }
        file_id
    }

    fn filter_objects_for_path<'a>(path: &'a str, objects: &'a [Object]) -> Vec<&'a Object> {
        objects
            .iter()
            .filter(|object| object.key.as_ref().unwrap().ends_with(path))
            .collect::<Vec<_>>()
    }

    fn name_without_slash(name: &str) -> String {
        name.replace("/", "")
    }

    fn add_child(&mut self, child: File) {
        match self.query(&child.path) {
            None => {
                self.children.push(child);
            }
            Some(_) => {}
        }
    }

    fn add_children(&mut self, children: Vec<File>) {
        for child in children.iter() {
            self.add_child(child.to_owned())
        }
    }

    fn query(&self, id: &str) -> Option<&Self> {
        if self.name == id {
            Some(self)
        } else {
            self.children
                .iter()
                .map(|x| x.query(id))
                .filter(|x| x.is_some())
                .flatten()
                .next()
        }
    }

    fn leaf_child(&self) -> Option<&File> {
        if self.path == self.prefix {
            Some(self)
        } else {
            self.children
                .iter()
                .map(|file| file.leaf_child())
                .next()
                .flatten()
        }
    }
}
