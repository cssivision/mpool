use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;
use mpool::{ManageConnection, Pool};
use tokio::net::TcpStream;

struct MyPool {
    addr: SocketAddr,
}

#[async_trait]
impl ManageConnection for MyPool {
    type Connection = TcpStream;

    async fn connect(&self) -> io::Result<Self::Connection> {
        TcpStream::connect(self.addr).await
    }

    async fn check(&self, _conn: &mut Self::Connection) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let manager = MyPool {
        addr: "127.0.0.1:8080".parse().unwrap(),
    };

    let pool = Pool::builder()
        .max_size(15)
        .idle_timeout(Some(Duration::from_secs(2)))
        .build(manager);
    let _conn = pool.get().await;
    Ok(())
}
