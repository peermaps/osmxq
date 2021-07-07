#![feature(async_closure)]
use async_std::{task,channel};
use osmxq::{XQ,Record,RecordId,Position};

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
      let brs: Vec<Box<dyn Record>> = records.iter().map(|r| r.lift()).collect();
      xq.add_records(&brs).await.unwrap();
    }
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
  fn lift(&self) -> Box<dyn Record> {
    Box::new(Clone::clone(self))
  }
}
