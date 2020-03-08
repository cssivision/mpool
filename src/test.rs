use std::io;
use std::time::Duration;

use crate::{ManageConnection, Pool};

use async_trait::async_trait;
use tokio::time::delay_for;

#[derive(Debug, PartialEq)]
struct FakeConnection;

#[derive(Default)]
struct FakeManager {
    sleep: Option<Duration>,
}

#[async_trait]
impl ManageConnection for FakeManager {
    type Connection = FakeConnection;

    async fn connect(&self) -> io::Result<Self::Connection> {
        if let Some(d) = self.sleep {
            delay_for(d).await;
        }

        Ok(FakeConnection)
    }

    async fn check(&self, _conn: &mut Self::Connection) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_max_size_ok() {
    let manager = FakeManager::default();
    let pool = Pool::builder().max_size(5).build(manager);
    let mut conns = vec![];
    for _ in 0..5 {
        conns.push(pool.get().await.unwrap());
    }
    assert_eq!(pool.interval().active, 5);
    assert!(pool.get().await.is_err());
    assert_eq!(pool.interval().active, 5);
}

#[tokio::test]
async fn test_drop_conn() {
    let manager = FakeManager::default();
    let pool = Pool::builder().max_size(2).build(manager);

    let conn1 = pool.get().await.unwrap();
    assert_eq!(pool.interval().active, 1);
    let conn2 = pool.get().await.unwrap();
    assert_eq!(pool.interval().active, 2);
    drop(conn1);
    assert_eq!(pool.interval().active, 2);
    drop(conn2);
    assert_eq!(pool.interval().active, 2);
    let _ = pool.get().await.unwrap();
    assert_eq!(pool.interval().active, 2);
}

#[tokio::test]
async fn test_get_timeout() {
    let manager = FakeManager {
        sleep: Some(Duration::from_millis(200)),
    };

    let pool = Pool::builder()
        .connection_timeout(Some(Duration::from_millis(100)))
        .build(manager);

    assert!(pool.get().await.is_err());
    assert!(pool
        .get_timeout(Some(Duration::from_millis(300)))
        .await
        .is_ok());
}

#[tokio::test]
async fn test_idle_timeout() {
    let manager = FakeManager::default();
    let pool = Pool::builder()
        .idle_timeout(Some(Duration::from_secs(1)))
        .check_interval(Some(Duration::from_secs(1)))
        .build(manager);

    let mut conns = vec![];
    for _ in 0..5 {
        conns.push(pool.get().await.unwrap());
    }
    drop(conns);
    delay_for(Duration::from_secs(2)).await;
    assert_eq!(pool.interval().active, 0);
}

#[tokio::test]
async fn test_max_lifetime() {
    let manager = FakeManager::default();
    let pool = Pool::builder()
        .max_lifetime(Some(Duration::from_secs(1)))
        .check_interval(Some(Duration::from_secs(1)))
        .build(manager);

    let mut conns = vec![];
    for _ in 0..5 {
        conns.push(pool.get().await.unwrap());
    }
    drop(conns);
    delay_for(Duration::from_secs(2)).await;
    assert_eq!(pool.interval().active, 0);
}
