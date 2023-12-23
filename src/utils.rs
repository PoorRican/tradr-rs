use std::env::temp_dir;
use std::fs::{create_dir_all, remove_dir_all};
use std::path::{Path, PathBuf};

/// create temp dir for testing
pub fn create_temp_dir(dir: &Path) -> PathBuf {

    let temp_dir = temp_dir();
    let path = temp_dir.join(dir);

    // delete dir if it already exists
    if path.exists() {
        remove_dir_all(&path).unwrap();
    }
    create_dir_all(path.clone()).unwrap();
    path
}
