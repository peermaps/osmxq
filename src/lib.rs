#![feature(hash_drain_filter)]
use std::collections::HashMap;
use lru::LruCache;
use async_std::{prelude::*,fs};
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
  storage: Box<dyn Storage<S>>,
  stores: LruCache<String,S>,
  root: QTree,
  quad_cache: LruCache<QuadId,HashMap<RecordId,R>>,
  quad_updates: HashMap<QuadId,HashMap<RecordId,R>>,
  id_cache: LruCache<IdBlock,HashMap<RecordId,QuadId>>,
  id_updates: HashMap<IdBlock,HashMap<RecordId,QuadId>>,
  missing_updates: Vec<R>,
  next_quad_id: QuadId,
}

impl<S,R> XQ<S,R> where S: RW, R: Record {
  pub async fn new(storage: Box<dyn Storage<S>>) -> Result<Self,Error> {
    // todo: read tree from storage
    let mut xq = Self {
      storage,
      root: QTree::Quad {
        id: 0,
        bbox: (-180.0,-90.0,180.0,90.0),
      },
      stores: LruCache::new(500),
      quad_cache: LruCache::new(10_000),
      quad_updates: HashMap::new(),
      id_cache: LruCache::new(10_000),
      id_updates: HashMap::new(),
      missing_updates: vec![],
      next_quad_id: 1,
    };
    xq.quad_updates.insert(0, HashMap::new());
    Ok(xq)
  }
  async fn insert_id(&mut self, id: RecordId, q_id: QuadId) -> Result<(),Error> {
    let b = id_block(id);
    let ifile = id_file(id);
    if let Some(ids) = self.id_updates.get_mut(&b) {
      ids.insert(id, q_id);
    } else if let Some(mut ids) = self.id_cache.pop(&b) {
      ids.insert(id, q_id);
      self.id_updates.insert(b, ids);
    } else if let Some(s) = self.stores.get_mut(&ifile) {
      let mut buf = Vec::new();
      s.read_to_end(&mut buf).await?;
      let mut ids = unpack_ids(&buf)?;
      ids.insert(id, q_id);
      self.id_updates.insert(b, ids);
      self.id_cache.pop(&b);
    } else {
      let mut s = self.storage.open(&ifile).await?;
      let mut buf = Vec::new();
      s.read_to_end(&mut buf).await?;
      let mut ids = unpack_ids(&buf)?;
      ids.insert(id, q_id);
      self.id_updates.insert(b, ids);
      self.stores.put(ifile, s);
    }
    Ok(())
  }
  pub async fn add_records(&mut self, records: &[R]) -> Result<(),Error> {
    let qs = self.get_quads(&records).await?;
    for (q_id,(bbox,ix)) in qs.iter() {
      for i in ix {
        let record = records.get(*i).unwrap();
        self.insert_id(record.get_id(), *q_id).await?;
      }
      let mut item_len = 0;
      self.quad_updates.get_mut(&q_id).map(|items| {
        for i in ix {
          let r = records.get(*i).unwrap();
          items.insert(r.get_id(), r.clone());
        }
        item_len = items.len();
        println!["{}", items.len()];
      });
      if item_len > 100_000 {
        self.split_quad(&q_id, &bbox).await?;
      }
    }
    self.check_flush().await?;
    Ok(())
  }
  pub async fn check_flush(&mut self) -> Result<(),Error> {
    if self.quad_updates.len() >= 100_000 && self.id_updates.len() >= 100_000 {
      // todo: parallel io
      self.quad_flush().await?;
      self.id_flush().await?;
    } else if self.quad_updates.len() >= 100_000 {
      self.quad_flush().await?;
    } else if self.id_updates.len() >= 100_000 {
      self.id_flush().await?;
    }
    Ok(())
  }
  pub async fn quad_flush(&mut self) -> Result<(),Error> {
    // todo: parallel io
    // todo: lock quad_updates
    for (q_id,rs) in self.quad_updates.drain() {
      let qfile = quad_file(q_id);
      if let Some(s) = self.stores.get_mut(&qfile) {
        s.write_all(&R::pack(&rs)).await?;
      } else {
        let mut s = self.storage.open(&qfile).await?;
        s.write_all(&R::pack(&rs)).await?;
        self.stores.put(qfile, s);
      }
      self.quad_cache.put(q_id,rs);
    }
    Ok(())
  }
  pub async fn id_flush(&mut self) -> Result<(),Error> {
    // todo: lock id_updates
    eprintln!["id_updates.len()={}", self.id_updates.len()];
    for (b,ids) in self.id_updates.drain() {
      let ifile = id_file_from_block(b);
      if let Some(s) = self.stores.get_mut(&ifile) {
        s.write_all(&pack_ids(&ids)).await?;
      } else {
        let mut s = self.storage.open(&ifile).await?;
        s.write_all(&pack_ids(&ids)).await?;
        self.stores.put(ifile, s);
      }
      self.id_cache.put(b,ids);
    }
    Ok(())
  }
  pub async fn flush(&mut self) -> Result<(),Error> {
    // todo: parallel io
    self.quad_flush().await?;
    self.id_flush().await?;
    Ok(())
  }
  pub async fn get_record(&mut self, id: RecordId) -> Result<Option<R>,Error> {
    let b = id_block(id);
    let mut o_q_id = self.id_updates.get(&b).and_then(|ids| ids.get(&id));
    if o_q_id.is_none() {
      o_q_id = self.id_cache.get(&b).and_then(|ids| ids.get(&id));
    }
    let q_id = if let Some(q_id) = o_q_id { *q_id } else {
      let ifile = id_file(id);
      let mut buf = Vec::new();
      if let Some(s) = self.stores.get_mut(&ifile) {
        s.read_to_end(&mut buf).await?;
      } else {
        let mut s = self.storage.open(&ifile).await?;
        s.read_to_end(&mut buf).await?;
        self.stores.put(ifile, s);
      };
      let ids = unpack_ids(&buf)?;
      let g = ids.get(&id).copied();
      self.id_cache.put(b, ids);
      if g.is_none() { return Ok(None) }
      g.unwrap()
    };
    if let Some(records) = self.quad_updates.get(&q_id) {
      return Ok(records.get(&id).cloned());
    }
    if let Some(records) = self.quad_cache.get(&q_id) {
      return Ok(records.get(&id).cloned());
    }
    let qfile = quad_file(q_id);
    let mut buf = Vec::new();
    if let Some(s) = self.stores.get_mut(&qfile) {
      s.read_to_end(&mut buf).await?;
    } else {
      let mut s = self.storage.open(&qfile).await?;
      s.read_to_end(&mut buf).await?;
      self.stores.put(qfile, s);
    }
    let records = R::unpack(&buf)?;
    let r = records.get(&id).cloned();
    self.quad_cache.put(q_id, records);
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
    // todo: lock quad_updates and quad_cache?
    let records = match (self.quad_updates.remove(q_id),self.quad_cache.pop(q_id)) {
      (Some(rs),_) => rs,
      (_,Some(rs)) => rs,
      (None,None) => unimplemented![], // todo: read from storage
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
        self.missing_updates.push(r);
      }
    }
    let mut i = 0;
    for q in quads {
      if i == 0 {
        for (r_id,_) in q.1.iter() {
          self.insert_id(*r_id, *q_id).await?;
        }
        self.quad_updates.insert(*q_id, q.1);
      } else {
        let id = self.next_quad_id;
        self.next_quad_id += 1;
        for (r_id,_) in q.1.iter() {
          self.insert_id(*r_id, id).await?;
        }
        self.quad_updates.insert(id, q.1);
      }
      i += 1;
    }
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
        self.missing_updates.push(r.clone());
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
  pub async fn finish(&mut self) -> Result<(),Error> {
    let mut prev_len = self.missing_updates.len();
    loop {
      println!["missing.len()={}", prev_len];
      let records = self.missing_updates.drain(..).collect::<Vec<_>>();
      self.add_records(&records).await?;
      let missing_len = self.missing_updates.len();
      if missing_len == 0 || missing_len == prev_len {
        break;
      }
      prev_len = self.missing_updates.len();
    }
    println!["skipped {}", self.missing_updates.len()];
    Ok(())
  }
}

impl<R> XQ<fs::File,R> where R: Record {
  pub async fn open_from_path(path: &str) -> Result<XQ<fs::File,R>,Error> {
    Ok(Self::new(Box::new(FileStorage::open_from_path(path).await?)).await?)
  }
}

fn overlap(p: &Position, bbox: &BBox) -> bool {
  p.0 >= bbox.0 && p.0 <= bbox.2 && p.1 >= bbox.1 && p.1 <= bbox.3
}
fn quad_file(q_id: QuadId) -> String {
  format!["q/{:02x}/{:x}",q_id%256,q_id/256]
}
fn id_file(id: RecordId) -> String {
  let b = id_block(id);
  format!["i/{:02x}/{:x}",b%256,b/256]
}
fn id_file_from_block(b: IdBlock) -> String {
  format!["i/{:02x}/{:x}",b%256,b/256]
}
fn id_block(id: RecordId) -> IdBlock {
  id/100_000
}
fn id_range(b: IdBlock) -> (RecordId,RecordId) {
  (b*100_000,(b+1)*100_000)
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
