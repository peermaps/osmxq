use std::collections::HashMap;
use lru::LruCache;
use async_std::fs;

mod storage;
use storage::{Storage,FileStorage,RW};

pub type Position = (f32,f32);
pub type BBox = (f32,f32,f32,f32);
pub type QuadId = u64;
pub type RecordId = u64;
pub type Error = Box<dyn std::error::Error+Send+Sync>;

pub trait Record: Send+Sync {
  fn get_id(&self) -> RecordId;
  fn get_refs<'a>(&'a self) -> &'a [RecordId];
  fn get_position(&self) -> Option<Position>;
  fn lift(&self) -> Box<dyn Record>;
}

pub enum QTree {
  Node { children: Vec<QTree>, bbox: BBox },
  Quad { id: QuadId, bbox: BBox },
}
impl QTree {
  pub fn bbox(&self) -> BBox {
    match self {
      QTree::Node { bbox, .. } => *bbox,
      QTree::Quad { bbox, .. } => *bbox,
    }
  }
}

pub struct XQ<S> where S: RW {
  storage: Box<dyn Storage<S>>,
  stores: LruCache<String,S>,
  root: QTree,
  quad_cache: LruCache<QuadId,Vec<Box<dyn Record>>>,
  quad_updates: HashMap<QuadId,Vec<Box<dyn Record>>>,
  id_cache: LruCache<RecordId,QuadId>,
  id_updates: HashMap<RecordId,QuadId>,
}

impl<S> XQ<S> where S: RW {
  pub async fn new(storage: Box<dyn Storage<S>>) -> Result<Self,Error> {
    // todo: read tree from storage
    Ok(Self {
      storage,
      root: QTree::Node {
        children: vec![],
        bbox: (-180.0,-90.0,180.0,90.0),
      },
      stores: LruCache::new(500),
      quad_cache: LruCache::new(10_000),
      quad_updates: HashMap::new(),
      id_cache: LruCache::new(10_000),
      id_updates: HashMap::new(),
    })
  }
  pub async fn add_record(&mut self, record: Box<dyn Record>) -> Result<(),Error> {
    let id = record.get_id();
    let q_id = self.get_quad(&record).await?.unwrap();
    self.id_updates.insert(id, q_id);
    self.quad_updates.get_mut(&q_id).map(|items| {
      items.push(record);
    });
    // todo: if items.len() > threshold { split_quad() }
    self.check_flush().await?;
    Ok(())
  }
  pub async fn check_flush(&mut self) -> Result<(),Error> {
    // todo: call flush() if over a threshold of updates
    Ok(())
  }
  pub async fn flush(&mut self) -> Result<(),Error> {
    // todo: write to storage and clear updates and cache
    Ok(())
  }
  pub async fn get_record(&mut self, id: RecordId) -> Result<Option<Box<dyn Record>>,Error> {
    let q_id = match (self.id_updates.get(&id),self.id_cache.get(&id)) {
      (Some(q_id),_) => q_id,
      (_,Some(q_id)) => q_id,
      (None,None) => unimplemented![], // todo: read from storage
    };
    if let Some(records) = self.quad_updates.get(q_id) {
      return Ok(records.iter().find(|r| r.get_id() == id).map(|r| (*r).lift()));
    }
    if let Some(records) = self.quad_cache.get(q_id) {
      return Ok(records.iter().find(|r| r.get_id() == id).map(|r| (*r).lift()));
    }
    // todo: read from storage
    unimplemented![]
  }
  async fn get_position(&mut self, record: &Box<dyn Record>) -> Result<Option<Position>,Error> {
    if let Some(p) = record.get_position() { return Ok(Some(p)) }
    let refs = record.get_refs();
    if refs.is_empty() { return Ok(None) }
    let o_r = self.get_record(*refs.first().unwrap()).await?;
    if o_r.is_none() { return Ok(None) }
    let record = o_r.unwrap();
    if let Some(p) = record.get_position() { return Ok(Some(p)) }
    let refs = record.get_refs();
    if refs.is_empty() { return Ok(None) }
    let o_r = self.get_record(*refs.first().unwrap()).await?;
    if o_r.is_none() { return Ok(None) }
    let record = o_r.unwrap();
    Ok(record.get_position())
  }
  pub async fn get_quad(&mut self, record: &Box<dyn Record>) -> Result<Option<QuadId>,Error> {
    let pos = {
      let p = self.get_position(record).await?;
      if p.is_none() { return Ok(None) }
      p.unwrap()
    };
    let mut c = &self.root;
    loop {
      match c {
        QTree::Node { children, .. } => {
          let o_n = children.iter().find(|ch| overlap(&pos,&ch.bbox()));
          if let Some(n) = o_n {
            c = n;
          } else {
            break;
          }
        },
        QTree::Quad { id, .. } => { return Ok(Some(*id)) }
      }
    }
    Ok(None)
  }
}

impl XQ<fs::File> {
  pub async fn open_from_path(path: &str) -> Result<XQ<fs::File>,Error> {
    Ok(Self::new(Box::new(FileStorage::open_from_path(path).await?)).await?)
  }
}

fn overlap(p: &Position, bbox: &BBox) -> bool {
  p.0 >= bbox.0 && p.0 <= bbox.2 && p.1 >= bbox.1 && p.1 <= bbox.3
}
