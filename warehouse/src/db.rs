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
        let result: Result<redis::Value, redis::RedisError> = redis::cmd("HGET")
            .arg(&[redis_key, "validated"])
            .query(conn.deref_mut());

        match result {
            Ok(redis::Value::Nil) => {
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

                                        if count >= good.count {
                                            pipe.cmd("HSET").arg(&[
                                                &format!("good_id:{}", good.id),
                                                "count",
                                                &(count - good.count).to_string(),
                                            ]);
                                        } else {
                                            error!(
                                                "Not enough good in warehouse with id: {}",
                                                self.goods[i].id
                                            );
                                            return;
                                        }
                                    }
                                    value => {
                                        error!("Redis server returned invalid value on HSET pipeline: {:?}", value);
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    value => {
                        error!(
                            "Redis server returned invalid value on HGET pipeline: {:?}",
                            value
                        );
                        return;
                    }
                }

                match pipe.query(conn.deref_mut()) {
                    Ok(x) => {
                        if let redis::Value::Bulk(_) = x {
                            let result: Result<redis::Value, redis::RedisError> =
                                redis::cmd("HSET")
                                    .arg(&[redis_key, "validated", "true"])
                                    .query(conn.deref_mut());

                            match result {
                                Ok(_) => {
                                    let result: Result<redis::Value, redis::RedisError> =
                                        redis::cmd("PERSIST")
                                            .arg(redis_key)
                                            .query(conn.deref_mut());

                                    match result {
                                        Ok(_) => (),
                                        Err(e) => error!(
                                            "Error happedned while executing HSET true on validated field, error: {}",
                                            e
                                        ),
                                    }
                                }
                                Err(e) => {
                                    error!("Error happedned while executing pipeline, error: {}", e)
                                }
                            }
                        }
                    }
                    Err(e) => error!(
                        "Error happedned while executing HSET pipeline on good_id count, error: {}",
                        e
                    ),
                }
            }
            Ok(value) => error!(
                "Redis HGET on validated field returned invalid value: {:?}",
                value
            ),
            Err(e) => error!(
                "Error happened while executing HGET cmd on validated field, error: {}",
                e
            ),
        }
    }
}
