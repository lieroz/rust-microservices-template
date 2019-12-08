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
                .arg(&[redis_key, "3600"])
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
                                return Ok(());
                            }
                        } else {
                            error!("{}:There is no good with id: {}", line!(), self.goods[i].id);
                            return Ok(());
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
        } else {
            warn!(
                "{}:Order with id: {} is already validated",
                line!(),
                self.id
            );
        }

        Ok(())
    }
}

#[derive(Deserialize)]
struct UpdateGood {
    id: u64,
    count: i64,
    operation: String,
}

#[derive(Deserialize)]
pub struct UpdateOrder {
    goods: Vec<UpdateGood>,
}

impl UpdateOrder {
    pub fn update(
        &self,
        user_id: &str,
        order_id: &str,
        conn: &mut r2d2::PooledConnection<RedisConnectionManager>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let redis_key = &format!("user_id:{}:order_id:{}", user_id, order_id);
        let result = redis::cmd("HGET")
            .arg(&[redis_key, "status"])
            .query(conn.deref_mut())?;

        if let redis::Value::Data(status) = result {
            let status = std::str::from_utf8(&status)?;

            if status == "created" {
                let mut pipe = redis::pipe();

                for good in &self.goods {
                    let good_id = &format!("good_id:{}", good.id);
                    let count = redis::cmd("HGET")
                        .arg(&[redis_key, good_id])
                        .query(conn.deref_mut())?;

                    if let redis::Value::Data(data) = count {
                        let count: i64 = std::str::from_utf8(&data)?.parse()?;

                        match &good.operation[..] {
                            "add" | "update" => {
                                let count = count - good.count;
                                pipe.cmd("HINCRBY")
                                    .arg(&[good_id, "count", &count.to_string()]);
                            }
                            "delete" => {
                                pipe.cmd("HINCRBY")
                                    .arg(&[good_id, "count", &count.to_string()]);
                            }
                            _ => error!("{}:Unknown operation: {}", line!(), good.operation),
                        }
                    }
                }

                let _ = pipe.query(conn.deref_mut())?;
            } else {
                error!(
                    "{}:Order id: {} with status: {} can't be updated",
                    line!(),
                    order_id,
                    status
                );
            }
        } else {
            error!("{}:There is no order with id: {}", line!(), order_id);
        }

        Ok(())
    }
}

pub fn delete_order(
    user_id: &str,
    order_id: &str,
    conn: &mut r2d2::PooledConnection<RedisConnectionManager>,
) -> Result<(), Box<dyn std::error::Error>> {
    let redis_key = &format!("user_id:{}:order_id:{}", user_id, order_id);
    let order = redis::cmd("HGETALL")
        .arg(&[redis_key])
        .query(conn.deref_mut())?;

    let mut pipe = redis::pipe();

    if let redis::Value::Bulk(bulk) = order {
        let mut i = 0;

        while i < bulk.len() {
            if let redis::Value::Data(data) = &bulk[i] {
                let key = std::str::from_utf8(&data)?;

                if key.contains("good_id") {
                    if let redis::Value::Data(data) = &bulk[i + 1] {
                        let value = std::str::from_utf8(&data)?;

                        pipe.cmd("HINCRBY").arg(&[key, "count", value]);
                        i += 2;
                    }
                } else {
                    i += 1;
                }
            }
        }
    } else {
        error!("Order with id: '{}' wasn't found", order_id);
    }

    let _ = pipe.query(conn.deref_mut())?;
    Ok(())
}
