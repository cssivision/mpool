use std::collections::LinkedList;
use std::io;
use std::ops::Add;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::time::{delay_for, timeout};

#[async_trait]
pub trait ManageConnection: Send + Sync + 'static {
    type Connection: Send + 'static;

    async fn connect(&self) -> io::Result<Self::Connection>;
    async fn check(&self, conn: &mut Self::Connection) -> io::Result<()>;
}

fn other(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::Other, msg)
}

pub struct Builder {
    pub max_lifetime: Option<Duration>,
    pub idle_timeout: Option<Duration>,
    pub dial_timeout: Option<Duration>,
    pub max_size: u32,
}

impl Default for Builder {
    fn default() -> Self {
        Builder {
            max_lifetime: Some(Duration::from_secs(60 * 30)),
            idle_timeout: Some(Duration::from_secs(60)),
            dial_timeout: Some(Duration::from_millis(1000)),
            max_size: 0,
        }
    }
}

impl Builder {
    pub fn new() -> Builder {
        Builder::default()
    }

    pub fn max_lifetime(mut self, max_lifetime: Option<Duration>) -> Self {
        self.max_lifetime = max_lifetime;
        self
    }

    pub fn idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    pub fn dial_timeout(mut self, dial_timeout: Option<Duration>) -> Self {
        self.dial_timeout = dial_timeout;
        self
    }

    pub fn max_size(mut self, max_size: u32) -> Self {
        self.max_size = max_size;
        self
    }

    pub fn build<M>(&self, manager: M) -> Pool<M>
    where
        M: ManageConnection,
    {
        let intervals = PoolInternals {
            conns: LinkedList::new(),
            active: 0,
        };

        let shared = SharedPool {
            intervals: Mutex::new(intervals),
            max_lifetime: self.max_lifetime,
            idle_timeout: self.idle_timeout,
            dial_timeout: self.dial_timeout,
            max_size: self.max_size,
            manager,
        };

        let pool = Pool(Arc::new(shared));
        tokio::spawn(pool.clone().check());
        pool
    }
}

pub struct Connection<M>
where
    M: ManageConnection,
{
    conn: Option<IdleConn<M::Connection>>,
    pool: Pool<M>,
}

impl<M> Drop for Connection<M>
where
    M: ManageConnection,
{
    fn drop(&mut self) {
        if self.conn.is_some() {
            self.pool.put(self.conn.take().unwrap());
        }
    }
}

impl<M> Deref for Connection<M>
where
    M: ManageConnection,
{
    type Target = M::Connection;

    fn deref(&self) -> &M::Connection {
        &self.conn.as_ref().unwrap().conn
    }
}

impl<M> DerefMut for Connection<M>
where
    M: ManageConnection,
{
    fn deref_mut(&mut self) -> &mut M::Connection {
        &mut self.conn.as_mut().unwrap().conn
    }
}

pub struct Pool<M>(Arc<SharedPool<M>>)
where
    M: ManageConnection;

impl<M> Clone for Pool<M>
where
    M: ManageConnection,
{
    fn clone(&self) -> Pool<M> {
        Pool(self.0.clone())
    }
}

impl<M> Pool<M>
where
    M: ManageConnection,
{
    fn interval<'a>(&'a self) -> MutexGuard<'a, PoolInternals<M::Connection>> {
        self.0.intervals.lock().unwrap()
    }

    fn idle_count(&self) -> usize {
        self.interval().conns.len()
    }

    fn incr_active(&self) {
        self.interval().active += 1;
    }

    fn decr_active(&self) {
        self.interval().active -= 1;
    }

    fn pop_front(&self) -> Option<IdleConn<M::Connection>> {
        self.interval().conns.pop_front()
    }

    fn push_back(&mut self, conn: IdleConn<M::Connection>) {
        self.interval().conns.push_back(conn);
    }

    fn exceed_idle_timeout(&self, conn: &IdleConn<M::Connection>) -> bool {
        if let Some(idle_timeout) = self.0.idle_timeout {
            if idle_timeout.as_micros() > 0 && conn.last_visited.add(idle_timeout) < Instant::now()
            {
                return true;
            }
        }

        false
    }

    fn exceed_max_lifetime(&self, conn: &IdleConn<M::Connection>) -> bool {
        if let Some(max_lifetime) = self.0.max_lifetime {
            if max_lifetime.as_micros() > 0 && conn.created.add(max_lifetime) < Instant::now() {
                return true;
            }
        }

        false
    }

    async fn check(mut self) {
        loop {
            delay_for(Duration::from_secs(3)).await;

            let n = self.idle_count();
            for _ in 0..n {
                if let Some(mut conn) = self.pop_front() {
                    if self.exceed_idle_timeout(&conn) || self.exceed_max_lifetime(&conn) {
                        self.decr_active();
                        continue;
                    }

                    match self.0.manager.check(&mut conn.conn).await {
                        Ok(_) => {
                            self.push_back(conn);
                            continue;
                        }
                        Err(_) => {
                            self.decr_active();
                        }
                    }
                    continue;
                }

                break;
            }
        }
    }

    fn exceed_limit(&self) -> bool {
        let max_size = self.0.max_size;
        if max_size > 0 && self.interval().active > max_size {
            true
        } else {
            false
        }
    }

    async fn dial_timeout(&self, dial_timeout: Option<Duration>) -> io::Result<M::Connection> {
        if let Some(dial_timeout) = dial_timeout {
            let conn = match timeout(dial_timeout, self.0.manager.connect()).await {
                Ok(s) => match s {
                    Ok(s) => s,
                    Err(e) => {
                        return Err(other(&e.to_string()));
                    }
                },
                Err(e) => {
                    return Err(other(&e.to_string()));
                }
            };

            Ok(conn)
        } else {
            let conn = self.0.manager.connect().await?;
            Ok(conn)
        }
    }

    pub async fn get(&self) -> io::Result<Connection<M>> {
        if let Some(conn) = self.pop_front() {
            return Ok(Connection {
                conn: Some(conn),
                pool: self.clone(),
            });
        }

        if self.exceed_limit() {
            return Err(other("exceed limit"));
        }

        let conn = self.dial_timeout(self.0.dial_timeout).await?;

        self.incr_active();
        return Ok(Connection {
            conn: Some(IdleConn {
                conn,
                last_visited: Instant::now(),
                created: Instant::now(),
            }),
            pool: self.clone(),
        });
    }

    fn put(&mut self, mut conn: IdleConn<M::Connection>) {
        conn.last_visited = Instant::now();
        self.push_back(conn);
    }
}

struct SharedPool<M>
where
    M: ManageConnection,
{
    intervals: Mutex<PoolInternals<M::Connection>>,
    max_lifetime: Option<Duration>,
    idle_timeout: Option<Duration>,
    dial_timeout: Option<Duration>,
    max_size: u32,
    manager: M,
}

struct IdleConn<C> {
    conn: C,
    last_visited: Instant,
    created: Instant,
}

struct PoolInternals<C> {
    conns: LinkedList<IdleConn<C>>,
    active: u32,
}
