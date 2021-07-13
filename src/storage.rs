use async_std::{io::{Read,Write},fs};
use std::path::PathBuf;

pub trait RW: Read+Write+Send+Sync+Unpin {}
impl RW for fs::File {}

type Error = Box<dyn std::error::Error+Send+Sync>;

#[async_trait::async_trait]
pub trait Storage<S>: Send+Sync+Unpin where S: RW {
  async fn open(&mut self, name: &str) -> Result<S,Error>;
  async fn remove(&mut self, name: &str) -> Result<(),Error>;
}

pub struct FileStorage {
  path: PathBuf,
}

impl FileStorage {
  pub async fn open_from_path<P>(path: P) -> Result<Self,Error>
  where PathBuf: From<P> {
    Ok(Self { path: path.into() })
  }
}

#[async_trait::async_trait]
impl Storage<fs::File> for FileStorage {
  async fn open(&mut self, name: &str) -> Result<fs::File,Error> {
    let p = self.path.join(name);
    fs::create_dir_all(p.parent().unwrap()).await?;
    Ok(fs::OpenOptions::new().read(true).write(true).create(true).open(p).await?)
  }
  async fn remove(&mut self, name: &str) -> Result<(),Error> {
    fs::remove_file(name).await.map_err(|e| e.into())
  }
}
