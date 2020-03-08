use std::io;

use crate::{ManageConnection, Pool};

use async_trait::async_trait;

#[derive(Debug, PartialEq)]
struct FakeConnection;

struct FakeManager;

#[async_trait]
impl ManageConnection for FakeManager {
    type Connection = FakeConnection;

    async fn connect(&self) -> io::Result<Self::Connection> {
        Ok(FakeConnection)
    }

    async fn check(&self, _conn: &mut Self::Connection) -> io::Result<()> {
        Ok(())
    }

    async fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

#[tokio::test]
async fn test_max_size_ok() {
    let manager = FakeManager;
    let pool = Pool::builder().max_size(5).build(manager);
    let mut conns = vec![];
    for _ in 0..5 {
        conns.push(pool.get().await.unwrap());
    }
    assert_eq!(pool.interval().active, 5);
    assert!(pool.get().await.is_err());
    assert_eq!(pool.interval().active, 5);
    drop(conns);
    assert_eq!(pool.interval().active, 0);
}
