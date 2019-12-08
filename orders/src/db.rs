use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use serde::Deserialize;
use std::ops::DerefMut;

#[derive(Deserialize)]
struct CreateGood {
    id: u64,
    count: u64,
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
            .arg(&[redis_key, "status"])
            .query(conn.deref_mut())?;

        if let redis::Value::Nil = result {
            let mut pipe = redis::pipe();
            pipe.cmd("HSET").arg(&[redis_key, "status", "created"]);

            for good in &self.goods {
                pipe.cmd("HSET").arg(&[
                    redis_key,
                    &format!("good_id:{}", good.id),
                    &good.count.to_string(),
                ]);
            }

            let _ = pipe.query(conn.deref_mut())?;
        } else {
            warn!("{}:Order with id: {} already exists", line!(), self.id);
        }

        Ok(())
    }
}

#[derive(Deserialize)]
struct UpdateGood {
    id: u64,
    count: u64,
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

                    match &good.operation[..] {
                        "add" | "update" => {
                            pipe.cmd("HSET")
                                .arg(&[redis_key, good_id, &good.count.to_string()]);
                        }
                        "delete" => {
                            pipe.cmd("HDEL").arg(&[redis_key, good_id]);
                        }
                        _ => error!("{}:Unknown operation: {}", line!(), good.operation),
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
        .arg(redis_key)
        .query(conn.deref_mut())?;

    if let redis::Value::Bulk(_) = order {
        let _ = redis::pipe()
            .cmd("HSET")
            .arg(&[redis_key, "status", "deleted"])
            .cmd("EXPIRE")
            .arg(&[redis_key, "3600"])
            .query(conn.deref_mut())?;
    } else {
        error!("Order with id: '{}' wasn't found", order_id);
    }

    Ok(())
}
