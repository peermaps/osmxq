use async_std::{io::{Read,Write,Seek},fs,path::PathBuf};

pub trait RW: Read+Write+Seek+Truncate+Send+Sync+Unpin {}
impl RW for fs::File {}

#[async_trait::async_trait]
pub trait Truncate {
  async fn set_len(&mut self, len: u64) -> Result<(),Error>;
}

type Error = Box<dyn std::error::Error+Send+Sync>;

#[async_trait::async_trait]
pub trait Storage<S>: Send+Sync+Unpin where S: RW {
  async fn open_rw(&mut self, name: &str) -> Result<S,Error>;
  async fn open_r(&mut self, name: &str) -> Result<Option<S>,Error>;
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
  async fn open_rw(&mut self, name: &str) -> Result<fs::File,Error> {
    let p = self.path.join(name);
    fs::create_dir_all(p.parent().unwrap()).await?;
    Ok(fs::OpenOptions::new().read(true).write(true).create(true).open(p).await?)
  }
  async fn open_r(&mut self, name: &str) -> Result<Option<fs::File>,Error> {
    let p = self.path.join(name);
    if p.exists().await {
      Ok(Some(fs::OpenOptions::new().read(true).open(p).await?))
    } else {
      Ok(None)
    }
  }
  async fn remove(&mut self, name: &str) -> Result<(),Error> {
    let p = self.path.join(name);
    fs::remove_file(p).await.map_err(|e| e.into())
  }
}

#[async_trait::async_trait]
impl Truncate for fs::File {
  async fn set_len(&mut self, len: u64) -> Result<(),Error> {
    fs::File::set_len(self, len).await.map_err(|e| e.into())
  }
}
