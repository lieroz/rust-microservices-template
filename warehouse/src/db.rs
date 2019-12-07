use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use serde::Deserialize;
use std::ops::DerefMut;

#[derive(Deserialize)]
struct CreateGood {
    id: u64,
    count: i64,
}

#[derive(Deserialize)]
pub struct CreateOrder {
    id: u64,
    goods: Vec<CreateGood>,
}

impl CreateOrder {
    pub fn create(
        &self,
        user_id: &str,
        conn: &mut r2d2::PooledConnection<RedisConnectionManager>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let redis_key = &format!("user_id:{}:order_id:{}", user_id, self.id);
        let result = redis::cmd("HGET")
            .arg(&[redis_key, "validated"])
            .query(conn.deref_mut())?;

        if let redis::Value::Nil = result {
            let mut pipe = redis::pipe();
            let _ = pipe
                .cmd("HSET")
                .arg(&[redis_key, "validated", "false"])
                .cmd("EXPIRE")
                .arg(&[redis_key, "60"])
                .query(conn.deref_mut())?;

            pipe = redis::pipe();

            for good in &self.goods {
                pipe.cmd("HGET")
                    .arg(&[&format!("good_id:{}", good.id), "count"]);
            }

            if let redis::Value::Bulk(bulk) = pipe.query(conn.deref_mut())? {
                if bulk.is_empty() {
                    error!("{}:There are no goods specified in order", line!());
                } else {
                    pipe = redis::pipe();

                    for (i, data) in bulk.iter().enumerate() {
                        if let redis::Value::Data(data) = data {
                            let count: i64 = std::str::from_utf8(&data).unwrap().parse().unwrap();
                            let good = &self.goods[i];

                            if count >= good.count {
                                pipe.cmd("HSET").arg(&[
                                    &format!("good_id:{}", good.id),
                                    "count",
                                    &(count - good.count).to_string(),
                                ]);
                            } else {
                                error!(
                                    "{}:Not enough good in warehouse with id: {}",
                                    line!(),
                                    self.goods[i].id
                                );
                            }
                        }
                    }

                    let _ = pipe
                        .cmd("HSET")
                        .arg(&[redis_key, "validated", "true"])
                        .cmd("PERSIST")
                        .arg(redis_key)
                        .query(conn.deref_mut())?;
                    let _ = pipe.query(conn.deref_mut())?;
                }
            }
        }

        Ok(())
    }
}
