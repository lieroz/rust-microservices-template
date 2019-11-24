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
    pub fn create(&self, user_id: &str, conn: &mut r2d2::PooledConnection<RedisConnectionManager>) {
        let redis_key = &format!("user_id:{}:order_id:{}", user_id, self.id);

        if let redis::Value::Bulk(bulk) = redis::cmd("HGETALL")
            .arg(redis_key)
            .query(conn.deref_mut())
            .unwrap()
        {
            if bulk.is_empty() {
                let mut pipe = redis::pipe();
                pipe.cmd("HSET").arg(&[redis_key, "status", "created"]);

                for good in &self.goods {
                    pipe.cmd("HSET").arg(&[
                        redis_key,
                        &format!("good_id:{}", good.id),
                        &good.count.to_string(),
                    ]);
                }

                match pipe.query(conn.deref_mut()).unwrap() {
                    redis::Value::Bulk(data) => {
                        for d in data {
                            match d {
                                redis::Value::Int(i) if i == 1 => continue,
                                value => error!("Error: redis returned invalid valud: {:?}", value),
                            }
                        }
                    }
                    value => error!("Error: redis server returned invalid value: {:?}", value),
                }
            } else {
                error!("Error: order with id: {} already exists", redis_key);
            }
        } else {
            error!("Error: redis returned invalid answer");
        }
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
    ) {
        let redis_key = &format!("user_id:{}:order_id:{}", user_id, order_id);

        if let redis::Value::Data(status) = redis::cmd("HGET")
            .arg(&[redis_key, "status"])
            .query(conn.deref_mut())
            .unwrap()
        {
            let status = std::str::from_utf8(&status).unwrap();

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
                        _ => warn!("Warning: unknown operation: {}", good.operation),
                    }
                }

                match pipe.query(conn.deref_mut()).unwrap() {
                    redis::Value::Bulk(data) => {
                        for d in data {
                            match d {
                                redis::Value::Int(_) => continue,
                                value => {
                                    error!("Error: redis should have returned Integer: {:?}", value)
                                }
                            }
                        }
                    }
                    value => error!("Error: redis server returned invalid value: {:?}", value),
                }
            } else {
                warn!("Warning: status is {}, order can't be updated", status);
            }
        } else {
            error!("Error: order with id: {} is not present", redis_key);
        }
    }
}

pub fn delete_order(
    user_id: &str,
    order_id: &str,
    conn: &mut r2d2::PooledConnection<RedisConnectionManager>,
) {
    let redis_key = &format!("user_id:{}:order_id:{}", user_id, order_id);

    if let redis::Value::Int(count) = redis::cmd("DEL")
        .arg(redis_key)
        .query(conn.deref_mut())
        .unwrap()
    {
        if count == 0 {
            warn!("Warning: order with id: {} couldn't be found", redis_key);
        }
    }
}
