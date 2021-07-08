#![feature(async_closure)]
use async_std::{task,channel};
use std::collections::HashMap;
use osmxq::{XQ,Record,RecordId,Position};
use desert::{varint,ToBytes,FromBytes,CountBytes};

type Error = Box<dyn std::error::Error+Send+Sync+'static>;

#[async_std::main]
async fn main() -> Result<(),Error> {
  let (_args,argv) = argmap::parse(std::env::args());
  let outdir = argv.get("o").or_else(|| argv.get("outdir"))
    .and_then(|xs| xs.first()).unwrap();
  let mut xq = XQ::open_from_path(&outdir).await?;

  let pbf_file = argv.get("i").or_else(|| argv.get("infile"))
    .and_then(|xs| xs.first()).unwrap();
  let pbf = std::fs::File::open(pbf_file)?;

  let (sender,receiver) = channel::bounded::<Vec<Feature>>(1_000);
  let mut work = vec![];
  work.push(task::spawn(async move {
    while let Ok(records) = receiver.recv().await {
      xq.add_records(&records).await.unwrap();
    }
    xq.finish().await.unwrap();
    xq.flush().await.unwrap();
  }));
  work.push(task::spawn(async move {
    let sc = sender.clone();
    let mut records = vec![];
    osmpbf::ElementReader::new(pbf).for_each(move |element| {
      let s = sc.clone();
      records.push(Feature::new(element));
      if records.len() >= 10_000 {
        let rs = records.clone();
        task::block_on(async move {
          s.send(rs).await.unwrap();
        });
        records.clear();
      }
    }).unwrap();
    sender.close();
  }));
  futures::future::join_all(work).await;
  Ok(())
}

#[derive(Clone,Debug)]
struct Feature {
  id: RecordId,
  refs: Vec<RecordId>,
  position: Option<Position>,
}

impl Feature {
  fn new<'a>(element: osmpbf::Element<'a>) -> Self {
    match element {
      osmpbf::Element::Node(node) => Self {
        id: node.id() as u64,
        refs: vec![],
        position: Some((node.lon() as f32, node.lat() as f32)),
      },
      osmpbf::Element::DenseNode(node) => Self {
        id: node.id() as u64,
        refs: vec![],
        position: Some((node.lon() as f32, node.lat() as f32)),
      },
      osmpbf::Element::Way(way) => Self {
        id: way.id() as u64,
        refs: way.refs().map(|r| r as u64).collect(),
        position: None,
      },
      osmpbf::Element::Relation(relation) => Self {
        id: relation.id() as u64,
        refs: relation.members().map(|m| m.member_id as u64).collect(),
        position: None,
      },
    }
  }
}

impl Record for Feature {
  fn get_id(&self) -> RecordId {
    self.id
  }
  fn get_refs<'a>(&'a self) -> &'a [RecordId] {
    &self.refs
  }
  fn get_position(&self) -> Option<Position> {
    self.position
  }
  fn pack(records: &HashMap<RecordId,Self>) -> Vec<u8> where Self: Sized {
    let mut size = 0;
    size += varint::length(records.len() as u64);
    for (r_id,r) in records {
      let xid = r_id*2 + if r.position.is_some() { 1 } else { 0 };
      size += varint::length(xid);
      size += r.position.map(|p| p.count_bytes()).unwrap_or(0);
      size += r.refs.count_bytes();
    }
    let mut buf = vec![0u8;size];
    let mut offset = 0;
    offset += varint::encode(records.len() as u64, &mut buf[offset..]).unwrap();
    for (r_id,r) in records {
      let xid = r_id*2 + if r.position.is_some() { 1 } else { 0 };
      offset += varint::encode(xid, &mut buf[offset..]).unwrap();
      if let Some(p) = r.position {
        offset += p.write_bytes(&mut buf[offset..]).unwrap();
      }
      offset += r.refs.write_bytes(&mut buf[offset..]).unwrap();
    }
    buf
  }
  fn unpack(buf: &[u8]) -> Result<HashMap<RecordId,Self>,Error> where Self: Sized {
    let mut records = HashMap::new();
    let mut offset = 0;
    let (s,record_len) = varint::decode(&buf[offset..])?;
    offset += s;
    for _ in 0..record_len {
      let (s,xid) = varint::decode(&buf[offset..])?;
      offset += s;
      let id = xid/2;
      let mut position = None;
      if xid % 2 == 1 {
        let (s,p) = Position::from_bytes(&buf[offset..])?;
        offset += s;
        position = Some(p);
      }
      let (s,refs) = <Vec<RecordId>>::from_bytes(&buf[offset..])?;
      offset += s;
      records.insert(id, Self { id, refs, position });
    }
    Ok(records)
  }
}
