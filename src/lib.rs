#![feature(hash_drain_filter)]
use std::collections::HashMap;
use lru::LruCache;
use async_std::{prelude::*,fs,sync::{RwLock,Arc,Mutex}};
use desert::varint;

mod storage;
use storage::{Storage,FileStorage,RW};

pub type Position = (f32,f32);
pub type BBox = (f32,f32,f32,f32);
pub type QuadId = u64;
pub type RecordId = u64;
pub type IdBlock = u64;
pub type Error = Box<dyn std::error::Error+Send+Sync>;

pub trait Record: Send+Sync+Clone+std::fmt::Debug {
  fn get_id(&self) -> RecordId;
  fn get_refs<'a>(&'a self) -> &'a [RecordId];
  fn get_position(&self) -> Option<Position>;
  fn pack(records: &HashMap<RecordId,Self>) -> Vec<u8> where Self: Sized;
  fn unpack(buf: &[u8]) -> Result<HashMap<RecordId,Self>,Error>;
}

#[derive(Debug)]
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

pub struct XQ<S,R> where S: RW, R: Record {
  storage: Mutex<Box<dyn Storage<S>>>,
  active_files: HashMap<String,Arc<Mutex<()>>>,
  root: QTree,
  quad_cache: LruCache<QuadId,HashMap<RecordId,R>>,
  quad_updates: Arc<RwLock<HashMap<QuadId,Option<HashMap<RecordId,R>>>>>,
  next_quad_id: QuadId,
  id_cache: LruCache<IdBlock,HashMap<RecordId,QuadId>>,
  id_updates: Arc<RwLock<HashMap<IdBlock,HashMap<RecordId,QuadId>>>>,
  missing_updates: HashMap<RecordId,R>,
  missing_count: usize,
  next_missing_id: u64,
  fields: Fields,
}

pub struct Fields {
  id_block_size: usize,
  id_cache_size: usize,
  id_flush_size: usize,
  quad_block_size: usize,
  quad_cache_size: usize,
  quad_flush_size: usize,
  missing_flush_size: usize,
}

impl Default for Fields {
  fn default() -> Self {
    Self {
      id_block_size: 500_000,
      id_cache_size: 2_000,
      id_flush_size: 500,
      quad_block_size: 50_000,
      quad_cache_size: 1_000,
      quad_flush_size: 500,
      missing_flush_size: 2_000_000,
    }
  }
}

impl<S,R> XQ<S,R> where S: RW, R: Record {
  pub async fn open(storage: Box<dyn Storage<S>>) -> Result<Self,Error> {
    Self::from_fields(storage, Fields::default()).await
  }
  pub async fn from_fields(storage: Box<dyn Storage<S>>, fields: Fields) -> Result<Self,Error> {
    // todo: read tree from storage
    let mut quad_updates = HashMap::new();
    quad_updates.insert(0, None);
    let xq = Self {
      storage: Mutex::new(storage),
      root: QTree::Quad {
        id: 0,
        bbox: (-180.0,-90.0,180.0,90.0),
      },
      active_files: HashMap::new(),
      quad_cache: LruCache::new(fields.quad_cache_size),
      quad_updates: Arc::new(RwLock::new(quad_updates)),
      id_cache: LruCache::new(fields.id_cache_size),
      id_updates: Arc::new(RwLock::new(HashMap::new())),
      missing_updates: HashMap::new(),
      missing_count: 0,
      next_quad_id: 1,
      next_missing_id: 0,
      fields,
    };
    Ok(xq)
  }
  async fn insert_id(&mut self, id: RecordId, q_id: QuadId) -> Result<(),Error> {
    let b = self.id_block(id);
    let ifile = self.id_file(id);
    if let Some(ids) = self.id_updates.write().await.get_mut(&b) {
      ids.insert(id, q_id);
      return Ok(());
    }
    if let Some(mut ids) = self.id_cache.pop(&b) {
      ids.insert(id, q_id);
      self.id_updates.write().await.insert(b, ids);
      return Ok(());
    }
    {
      let mut s = self.open_file(&ifile).await?;
      if let Some(mut ids) = self.id_cache.pop(&b) {
        ids.insert(id, q_id);
        self.id_updates.write().await.insert(b, ids);
        return Ok(());
      }
      let mut buf = Vec::new();
      s.read_to_end(&mut buf).await?;
      let mut ids = unpack_ids(&buf)?;
      ids.insert(id, q_id);
      self.id_updates.write().await.insert(b, ids);
      self.close_file(&ifile);
    }
    Ok(())
  }
  pub async fn add_records(&mut self, records: &[R]) -> Result<(),Error> {
    if records.is_empty() { return Ok(()) }
    let qs = self.get_quads(&records).await?;
    for (q_id,(bbox,ix)) in qs.iter() {
      for i in ix {
        let record = records.get(*i).unwrap();
        self.insert_id(record.get_id(), *q_id).await?;
      }
      let mut item_len = 0;
      self.quad_updates.write().await.get_mut(&q_id).map(|o_items| {
        if o_items.is_none() {
          *o_items = Some(HashMap::new());
        }
        let items = o_items.as_mut().unwrap();
        for i in ix {
          let r = records.get(*i).unwrap();
          items.insert(r.get_id(), r.clone());
        }
        item_len = items.len();
      });
      if item_len > self.fields.quad_block_size {
        self.split_quad(&q_id, &bbox).await?;
      }
    }
    self.check_flush().await?;
    Ok(())
  }
  pub async fn check_flush(&mut self) -> Result<(),Error> {
    // todo: parallel io
    if self.quad_updates.read().await.len() >= self.fields.quad_flush_size {
      self.quad_flush().await?;
    }
    if self.id_updates.read().await.len() >= self.fields.id_flush_size {
      self.id_flush().await?;
    }
    if self.missing_updates.len() >= self.fields.missing_flush_size {
      self.missing_flush().await?;
    }
    Ok(())
  }
  pub async fn quad_flush(&mut self) -> Result<(),Error> {
    // todo: parallel io
    let qu = self.quad_updates.clone();
    for (q_id,o_rs) in qu.write().await.drain() {
      if o_rs.is_none() { continue }
      let rs = o_rs.unwrap();
      let qfile = quad_file(q_id);
      let mut s = self.open_file(&qfile).await?;
      s.write_all(&R::pack(&rs)).await?;
      s.flush().await?;
      self.quad_cache.put(q_id,rs);
      self.close_file(&qfile);
    }
    Ok(())
  }
  pub async fn id_flush(&mut self) -> Result<(),Error> {
    eprintln!["id_updates.len()={}", self.id_updates.read().await.len()];
    let iu = self.id_updates.clone();
    for (b,ids) in iu.write().await.drain() {
      let ifile = id_file_from_block(b);
      let mut s = self.open_file(&ifile).await?;
      s.write_all(&pack_ids(&ids)).await?;
      s.flush().await?;
      self.id_cache.put(b,ids);
      self.close_file(&ifile);
    }
    Ok(())
  }
  pub async fn missing_flush(&mut self) -> Result<(),Error> {
    if self.missing_updates.is_empty() { return Ok(()) }
    let m_id = self.next_missing_id;
    self.next_missing_id += 1;
    let mfile = missing_file(m_id);
    let mut s = self.open_file(&mfile).await?;
    s.write_all(&R::pack(&self.missing_updates)).await?;
    s.flush().await?;
    self.missing_updates.clear();
    self.close_file(&mfile);
    Ok(())
  }
  pub async fn flush(&mut self) -> Result<(),Error> {
    // todo: parallel io
    self.quad_flush().await?;
    self.id_flush().await?;
    self.missing_flush().await?;
    Ok(())
  }
  pub async fn get_record(&mut self, id: RecordId) -> Result<Option<R>,Error> {
    let b = self.id_block(id);
    let mut o_q_id = self.id_updates.read().await.get(&b).and_then(|ids| ids.get(&id).copied());
    if o_q_id.is_none() {
      o_q_id = self.id_cache.get(&b).and_then(|ids| ids.get(&id)).copied();
    }
    let q_id = if let Some(q_id) = o_q_id { q_id } else {
      let ifile = self.id_file(id);
      let mut buf = Vec::new();
      let mut s = self.open_file(&ifile).await?;
      s.read_to_end(&mut buf).await?;
      let ids = unpack_ids(&buf)?;
      let g = ids.get(&id).copied();
      self.id_cache.put(b, ids);
      self.close_file(&ifile);
      if g.is_none() { return Ok(None) }
      g.unwrap()
    };
    if let Some(records) = self.quad_updates.read().await.get(&q_id) {
      return Ok(records.as_ref().and_then(|items| items.get(&id).cloned()));
    }
    if let Some(records) = self.quad_cache.get(&q_id) {
      return Ok(records.get(&id).cloned());
    }
    let qfile = quad_file(q_id);
    let mut buf = Vec::new();
    let mut s = self.open_file(&qfile).await?;
    s.read_to_end(&mut buf).await?;
    let records = R::unpack(&buf)?;
    let r = records.get(&id).cloned();
    self.quad_cache.put(q_id, records);
    self.close_file(&qfile);
    Ok(r)
  }
  async fn get_position(&mut self, record: &R) -> Result<Option<Position>,Error> {
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
  pub async fn split_quad(&mut self, q_id: &QuadId, bbox: &BBox) -> Result<(),Error> {
    let qfile = quad_file(*q_id);
    let mut s = self.open_file(&qfile).await?;

    let records = {
      let qu = self.quad_updates.write().await.remove(q_id);
      let qc = self.quad_cache.pop(q_id);
      match (qu,qc) {
        (Some(Some(rs)),_) => rs,
        (Some(None),_) => panic!["tried to split an empty quad"],
        (_,Some(rs)) => rs,
        (None,None) => {
          let mut buf = Vec::new();
          s.read_to_end(&mut buf).await?;
          R::unpack(&buf)?
        },
      }
    };
    let (nx,ny) = (4,4);
    let mut quads = vec![];
    for i in 0..nx {
      for j in 0..ny {
        let b = (
          bbox.0 + (i as f32/(nx as f32))*(bbox.2-bbox.0),
          bbox.1 + (j as f32/(nx as f32))*(bbox.3-bbox.1),
          bbox.0 + ((i+1) as f32/(nx as f32))*(bbox.2-bbox.0),
          bbox.1 + ((j+1) as f32/(nx as f32))*(bbox.3-bbox.1),
        );
        quads.push((b,HashMap::new()));
      }
    }
    for (r_id,r) in records {
      if let Some(p) = self.get_position(&r).await? {
        let i = quads.iter().position(|(b,_)| overlap(&p, &b)).unwrap();
        let q = quads.get_mut(i).unwrap();
        q.1.insert(r_id,r);
      } else {
        self.missing_updates.insert(r.get_id(),r);
        self.missing_count += 1;
      }
    }
    let mut i = 0;
    for q in quads {
      if i == 0 {
        for (r_id,_) in q.1.iter() {
          self.insert_id(*r_id, *q_id).await?;
        }
        if q.1.is_empty() {
          self.quad_updates.write().await.insert(*q_id, None);
        } else {
          self.quad_updates.write().await.insert(*q_id, Some(q.1));
        }
      } else {
        let id = self.next_quad_id;
        self.next_quad_id += 1;
        for (r_id,_) in q.1.iter() {
          self.insert_id(*r_id, id).await?;
        }
        if q.1.is_empty() {
          self.quad_updates.write().await.insert(id, None);
        } else {
          self.quad_updates.write().await.insert(id, Some(q.1));
        }
      }
      i += 1;
    }
    self.close_file(&qfile);
    self.check_flush().await?;
    Ok(())
  }
  pub async fn get_quads(&mut self, records: &[R])
  -> Result<HashMap<QuadId,(BBox,Vec<usize>)>,Error> {
    let mut result: HashMap<QuadId,(BBox,Vec<usize>)> = HashMap::new();
    let mut positions = HashMap::new();
    for (i,r) in records.iter().enumerate() {
      if let Some(p) = self.get_position(r).await? {
        positions.insert(i,p);
      } else {
        self.missing_updates.insert(r.get_id(),r.clone());
        self.missing_count += 1;
      }
    }
    let mut cursors = vec![&self.root];
    let mut ncursors = vec![];
    while !cursors.is_empty() {
      ncursors.clear();
      for c in cursors.iter() {
        match c {
          QTree::Node { children, .. } => {
            ncursors.extend(children.iter()
              .filter(|ch| {
                positions.iter().any(|(_,p)| { overlap(p,&ch.bbox()) })
              }).collect::<Vec<_>>());
          },
          QTree::Quad { id, bbox } => {
            positions.drain_filter(|i,p| {
              if overlap(p,bbox) {
                if let Some((_,items)) = result.get_mut(id) {
                  items.push(*i);
                } else {
                  result.insert(*id, (bbox.clone(),vec![*i]));
                }
                true
              } else {
                false
              }
            });
          }
        }
      }
      let tmp = ncursors;
      ncursors = cursors;
      cursors = tmp;
    }
    Ok(result)
  }
  async fn open_file(&mut self, file: &String) -> Result<S,Error> {
    if let Some(active) = self.active_files.get(file) {
      active.lock().await;
    }
    let mut st = self.storage.lock().await;
    let active = Arc::new(Mutex::new(()));
    let ac = active.clone();
    self.active_files.insert(file.clone(), active);
    ac.lock().await;
    st.open(file).await
  }
  fn close_file(&mut self, file: &String) {
    self.active_files.remove(file);
  }
  pub async fn finish(&mut self) -> Result<(),Error> {
    println!["finish"];
    let mut prev_count = 0;
    let mut missing_start = 0;
    loop {
      println!["missing_count={}", self.missing_count];
      let missing_end = self.next_missing_id;
      let m_records = self.missing_updates.drain().map(|(_,r)| r).collect::<Vec<_>>();
      for i in missing_start..missing_end {
        let mfile = missing_file(i);
        println!["i={} mfile={}", i, &mfile];
        let mut buf = Vec::new();
        {
          let mut s = self.open_file(&mfile).await?;
          s.read_to_end(&mut buf).await?;
          self.close_file(&mfile);
        }
        let mut records = R::unpack(&buf)?;
        self.add_records(&records.drain().map(|(_,r)| r).collect::<Vec<R>>()).await?;
      }
      println!["add_records {}", m_records.len()];
      self.add_records(&m_records).await?;
      if self.missing_count == 0 || self.missing_count == prev_count {
        println!["skipped {}", self.missing_count];
        break;
      }
      prev_count = self.missing_count;
      self.missing_count = 0;
      missing_start = missing_end;
    }
    self.missing_count = 0;
    // todo: delete missing files
    /*
    {
      let st = self.storage.lock().await;
      for i in 0..self.next_missing_id {
        st.remove(&missing_file(i)).await?;
      }
    }
    */
    Ok(())
  }
  fn id_block(&self, id: RecordId) -> IdBlock {
    id/(self.fields.id_block_size as u64)
  }
  fn id_file(&self, id: RecordId) -> String {
    let b = self.id_block(id);
    format!["i/{:02x}/{:x}",b%256,b/256]
  }
}

impl<R> XQ<fs::File,R> where R: Record {
  pub async fn open_from_path(path: &str) -> Result<XQ<fs::File,R>,Error> {
    Ok(Self::open(Box::new(FileStorage::open_from_path(path).await?)).await?)
  }
}

fn overlap(p: &Position, bbox: &BBox) -> bool {
  p.0 >= bbox.0 && p.0 <= bbox.2 && p.1 >= bbox.1 && p.1 <= bbox.3
}
fn quad_file(q_id: QuadId) -> String {
  format!["q/{:02x}/{:x}",q_id%256,q_id/256]
}
fn missing_file(m_id: u64) -> String {
  format!["m/{:x}",m_id]
}
fn id_file_from_block(b: IdBlock) -> String {
  format!["i/{:02x}/{:x}",b%256,b/256]
}
fn unpack_ids(buf: &[u8]) -> Result<HashMap<RecordId,QuadId>,Error> {
  let mut records = HashMap::new();
  if buf.is_empty() { return Ok(records) }
  let mut offset = 0;
  let (s,len) = varint::decode(&buf[offset..])?;
  offset += s;
  for _ in 0..len {
    let (s,r_id) = varint::decode(&buf[offset..])?;
    offset += s;
    let (s,q_id) = varint::decode(&buf[offset..])?;
    offset += s;
    records.insert(r_id, q_id);
  }
  Ok(records)
}
fn pack_ids(records: &HashMap<RecordId,QuadId>) -> Vec<u8> {
  let mut size = 0;
  size += varint::length(records.len() as u64);
  for (r_id,q_id) in records {
    size += varint::length(*r_id);
    size += varint::length(*q_id);
  }
  let mut buf = vec![0;size];
  let mut offset = 0;
  offset += varint::encode(records.len() as u64, &mut buf[offset..]).unwrap();
  for (r_id,q_id) in records {
    offset += varint::encode(*r_id, &mut buf[offset..]).unwrap();
    offset += varint::encode(*q_id, &mut buf[offset..]).unwrap();
  }
  buf
}
