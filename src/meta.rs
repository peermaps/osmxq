use desert::{varint,ToBytes,CountBytes,FromBytes};
use crate::{QuadId,QTree,BBox,Error};

pub struct Meta {
  pub next_quad_id: QuadId,
  pub id_block_size: usize,
  pub quad_block_size: usize,
  pub root: QTree,
}

impl ToBytes for Meta {
  fn to_bytes(&self) -> Result<Vec<u8>,Error> {
    let mut buf = vec![0;self.count_bytes()];
    self.write_bytes(&mut buf)?;
    Ok(buf)
  }
  fn write_bytes(&self, buf: &mut [u8]) -> Result<usize,Error> {
    let mut offset = 0;
    offset += varint::encode(self.next_quad_id, &mut buf[offset..])?;
    offset += varint::encode(self.id_block_size as u64, &mut buf[offset..])?;
    offset += varint::encode(self.quad_block_size as u64, &mut buf[offset..])?;

    fn write(node: &QTree, buf: &mut [u8]) -> Result<usize,Error> {
      let mut offset = 0;
      match node {
        QTree::Node { bbox, children } => {
          offset += bbox.write_bytes(&mut buf[offset..])?;
          offset += varint::encode((children.len() as u64)*2+0, &mut buf[offset..])?;
          for c in children {
            offset += write(c, &mut buf[offset..])?;
          }
        },
        QTree::Quad { bbox, id } => {
          offset += bbox.write_bytes(&mut buf[offset..])?;
          offset += varint::encode(id*2+0, &mut buf[offset..])?;
        },
      }
      Ok(offset)
    }
    write(&self.root, &mut buf[offset..])
  }
}

impl CountBytes for Meta {
  fn count_bytes(&self) -> usize {
    let mut size = 0;
    size += varint::length(self.next_quad_id);
    size += varint::length(self.id_block_size as u64);
    size += varint::length(self.quad_block_size as u64);
    
    fn count(node: &QTree) -> usize {
      match node {
        QTree::Node { bbox, children } => {
          bbox.count_bytes() + varint::length((children.len() as u64)*2+0)
            + children.iter().fold(0, |sum,c| sum + count(c))
        },
        QTree::Quad { bbox, id } => {
          bbox.count_bytes() + varint::length(id*2+1)
        },
      }
    }
    count(&self.root)
  }
  fn count_from_bytes(buf: &[u8]) -> Result<usize,Error> {
    unimplemented![]
  }
}

impl FromBytes for Meta {
  fn from_bytes(buf: &[u8]) -> Result<(usize,Self),Error> {
    let mut offset = 0;
    let (s,next_quad_id) = varint::decode(&buf[offset..])?;
    offset += s;
    let (s,id_block_size) = varint::decode(&buf[offset..])?;
    offset += s;
    let (s,quad_block_size) = varint::decode(&buf[offset..])?;
    offset += s;

    fn parse(buf: &[u8]) -> Result<(usize,QTree),Error> {
      let mut offset = 0;
      let (s,bbox) = BBox::from_bytes(&buf[offset..])?;
      offset += s;
      let (s,x) = varint::decode(&buf[offset..])?;
      offset += s;
      Ok(match x%2 {
        0 => {
          let mut children = Vec::with_capacity((x/2) as usize);
          for _ in 0..x/2 {
            let (s,c) = parse(&buf[offset..])?;
            offset += s;
            children.push(c);
          }
          (offset, QTree::Node { bbox, children })
        },
        _ => (offset, QTree::Quad { bbox, id: x/2 }),
      })
    }
    let (s,root) = parse(&buf[offset..])?;
    offset += s;
    Ok((offset, Self {
      next_quad_id,
      id_block_size: id_block_size as usize,
      quad_block_size: quad_block_size as usize,
      root
    }))
  }
}
