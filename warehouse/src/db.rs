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
    pub fn create(&self, user_id: &str, conn: &mut r2d2::PooledConnection<RedisConnectionManager>) {
        let redis_key = &format!("user_id:{}:order_id:{}", user_id, self.id);
        let mut pipe = redis::pipe();
        let result: Result<redis::Value, redis::RedisError> = pipe
            .cmd("HSET")
            .arg(&[redis_key, "validated", "false"])
            .cmd("EXPIRE")
            .arg(&[redis_key, "60"])
            .query(conn.deref_mut());

        match result {
            Ok(_) => (),
            Err(e) => error!(
                "Error happedned while executing HSET command on key: {}, error: {}",
                redis_key, e
            ),
        }

        pipe = redis::pipe();

        for good in &self.goods {
            pipe.cmd("HGET")
                .arg(&[&format!("good_id:{}", good.id), "count"]);
        }

        match pipe.query(conn.deref_mut()).unwrap() {
            redis::Value::Bulk(bulk) => {
                if bulk.is_empty() {
                    error!("There are no goods specified in order");
                    return;
                } else {
                    pipe = redis::pipe();

                    for (i, data) in bulk.iter().enumerate() {
                        match data {
                            redis::Value::Data(data) => {
                                let count: i64 =
                                    std::str::from_utf8(&data).unwrap().parse().unwrap();
                                let good = &self.goods[i];

                                if good.count >= count {
                                    pipe.cmd("HSET").arg(&[
                                        &format!("good_id:{}", good.id),
                                        "count",
                                        &(good.count - count).to_string(),
                                    ]);
                                } else {
                                    error!("Not enough goods with id: {}", self.goods[i].id);
                                    return;
                                }
                            }
                            value => {
                                error!("Redis server returned invalid value: {:?}", value);
                                return;
                            }
                        }
                    }
                }
            }
            value => {
                error!("Redis server returned invalid value: {:?}", value);
                return;
            }
        }

        match pipe.query(conn.deref_mut()) {
            Ok(x) => if let redis::Value::Bulk(_) = x {},
            Err(e) => error!("Error happedned while executing pipeline, error: {}", e),
        }

        let result: Result<redis::Value, redis::RedisError> = redis::cmd("HSET")
            .arg(&[redis_key, "validated", "true"])
            .query(conn.deref_mut());

        match result {
            Ok(_) => (),
            Err(e) => error!("Error happedned while executing pipeline, error: {}", e),
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
                        _ => warn!("Unknown operation: {}", good.operation),
                    }
                }

                match pipe.query(conn.deref_mut()).unwrap() {
                    redis::Value::Bulk(data) => {
                        for d in data {
                            match d {
                                redis::Value::Int(_) => continue,
                                value => error!("Redis should have returned Integer: {:?}", value),
                            }
                        }
                    }
                    value => error!("Redis server returned invalid value: {:?}", value),
                }
            } else {
                warn!("Order status is {}, order can't be updated", status);
            }
        } else {
            error!("Order with id: {} is not present", redis_key);
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
            warn!("Order with id: {} couldn't be found", redis_key);
        }
    }
}
