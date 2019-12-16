use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::ops::DerefMut;

#[derive(Deserialize)]
struct CreateGood {
    id: u64,
    count: u64,
}

#[derive(Deserialize)]
pub struct CreateOrder {
    goods: Vec<CreateGood>,
}

impl CreateOrder {
    pub fn create(
        &self,
        user_id: &str,
        conn: &mut r2d2::PooledConnection<RedisConnectionManager>,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let order_id = redis::cmd("INCR").arg("order_id").query(conn.deref_mut())?;
        let redis_key = &format!("tx:user_id:{}:order_id:{}", user_id, order_id);

        let mut pipe = redis::pipe();
        pipe.cmd("MULTI")
            .cmd("HSET")
            .arg(&[redis_key, "status", "new"]);

        for good in &self.goods {
            pipe.cmd("HSET").arg(&[
                redis_key,
                &format!("good_id:{}", good.id),
                &good.count.to_string(),
            ]);
        }

        let _ = pipe.cmd("EXEC").query(conn.deref_mut())?;

        Ok(order_id)
    }
}

#[derive(Serialize, Deserialize)]
struct UpdateGood {
    id: u64,
    #[serde(default)]
    count: i64,
    operation: String,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateOrder {
    goods: Vec<UpdateGood>,
}

const EXEC_TX: &str = r#"
    if redis.call('EXISTS', KEYS[2]) == 1 then
        return { err = 'Transaction '..KEYS[2]..' already exists'}
    end
    local hash = redis.call('HGETALL', KEYS[1]);
    if #hash == 0 then
        return { err = 'The key '..KEYS[1]..' does not exist' }
    end
    return redis.call('HMSET', KEYS[2], unpack(hash))"#;

// TODO: consider wrapping all operations with MULTI/EXEC to make operation atomic
// right now if redis is down, then it will make some temporary object persist
// TODO: order can be empty after update, consider fixing it
impl UpdateOrder {
    pub fn update(
        &mut self,
        user_id: &str,
        order_id: &str,
        conn: &mut r2d2::PooledConnection<RedisConnectionManager>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let order_key = &format!("user_id:{}:order_id:{}", user_id, order_id);
        let tx_key = &format!("tx:{}", order_key);

        let status: String = redis::cmd("HGET")
            .arg(&[order_key, "status"])
            .query(conn.deref_mut())?;

        if status == "payed" {
            return Err(Box::new(Error::new(
                ErrorKind::Other,
                format!(
                    "line:{}: Order '{}' can't be updated, cause it was already payed",
                    line!(),
                    order_id
                ),
            )));
        }

        let status: String = redis::cmd("EVAL")
            .arg(&[EXEC_TX, "2", order_key, tx_key])
            .query(conn.deref_mut())?;

        if status == "OK" {
            let mut pipe = redis::pipe();

            for good in &mut self.goods {
                let good_id = &format!("good_id:{}", good.id);
                let redis_count = redis::cmd("HGET")
                    .arg(&[order_key, good_id])
                    .query(conn.deref_mut())?;

                let redis_count = match redis_count {
                    redis::Value::Nil => 0,
                    redis::Value::Data(data) => std::str::from_utf8(&data)?.parse::<i64>()?,
                    value => {
                        return Err(Box::new(Error::new(
                            ErrorKind::Other,
                            format!("Invalid value: {:?}", value),
                        )))
                    }
                };

                match &good.operation[..] {
                    "update" => {
                        pipe.cmd("HSET")
                            .arg(&[tx_key, good_id, &good.count.to_string()]);
                    }
                    "delete" => {
                        pipe.cmd("HDEL").arg(&[tx_key, good_id]);
                    }
                    _ => {
                        // consider making one place to execute this code
                        // right now it looks like crutch
                        let _ = redis::cmd("DEL").arg(tx_key).query(conn.deref_mut())?;
                        return Err(Box::new(Error::new(
                            ErrorKind::Other,
                            format!("line:{}: Unknown operation: {}", line!(), good.operation),
                        )));
                    }
                }

                good.count = redis_count - good.count;
            }

            let _ = pipe.query(conn.deref_mut())?;
        } else {
            let _ = redis::cmd("DEL").arg(tx_key).query(conn.deref_mut())?;
            return Err(Box::new(Error::new(
                ErrorKind::Other,
                format!(
                    "line:{}: Redis returned invalid status: {}",
                    line!(),
                    status
                ),
            )));
        }

        Ok(())
    }
}

// TODO: if order is payed and was deleted, then billing must be rollout
// right now there is no logic with billing, so no rollout
pub fn delete_order(
    user_id: &str,
    order_id: &str,
    conn: &mut r2d2::PooledConnection<RedisConnectionManager>,
) -> Result<HashMap<String, u64>, Box<dyn std::error::Error>> {
    let order_key = &format!("user_id:{}:order_id:{}", user_id, order_id);
    let tx_key = &format!("tx:{}", order_key);

    let status: String = redis::cmd("EVAL")
        .arg(&[EXEC_TX, "2", order_key, tx_key])
        .query(conn.deref_mut())?;

    if status == "OK" {
        let order: Vec<redis::Value> = redis::cmd("HGETALL")
            .arg(&[tx_key])
            .query(conn.deref_mut())?;

        let mut map = HashMap::new();
        let mut i = 0;

        while i < order.len() {
            if let redis::Value::Data(data) = &order[i] {
                let key = std::str::from_utf8(&data)?;

                if key.contains("good_id") {
                    if let redis::Value::Data(data) = &order[i + 1] {
                        let value = std::str::from_utf8(&data)?;

                        map.insert(key.to_string(), value.parse::<u64>()?);
                        i += 2;
                    }
                } else {
                    i += 1;
                }
            }
        }

        Ok(map)
    } else {
        Err(Box::new(Error::new(
            ErrorKind::Other,
            format!(
                "line:{}: Redis returned invalid status: {}",
                line!(),
                status
            ),
        )))
    }
}

pub fn make_billing(
    user_id: &str,
    order_id: &str,
    conn: &mut r2d2::PooledConnection<RedisConnectionManager>,
) -> Result<(), Box<dyn std::error::Error>> {
    let order_key = &format!("user_id:{}:order_id:{}", user_id, order_id);
    let tx_key = &format!("tx:{}", order_key);

    let order_exists: i32 = redis::cmd("EXISTS")
        .arg(order_key)
        .query(conn.deref_mut())?;

    if order_exists == 1 {
        let tx_exists: i32 = redis::cmd("EXISTS").arg(tx_key).query(conn.deref_mut())?;

        if tx_exists == 1 {
            Err(Box::new(Error::new(
                ErrorKind::Other,
                format!(
                    "line:{}: Billing can't be made, there is unfinished transaction: {}",
                    line!(),
                    tx_key
                ),
            )))
        } else {
            let status: String = redis::cmd("HGET")
                .arg(&[order_key, "status"])
                .query(conn.deref_mut())?;

            if status != "payed" {
                let _ = redis::cmd("HSET")
                    .arg(&[order_key, "status", "payed"])
                    .query(conn.deref_mut())?;
            } else {
                return Err(Box::new(Error::new(
                    ErrorKind::Other,
                    format!(
                        "line:{}: Order with id: {} is already payed",
                        line!(),
                        order_id
                    ),
                )));
            }

            Ok(())
        }
    } else {
        Err(Box::new(Error::new(
            ErrorKind::Other,
            format!(
                "line:{}: Billing can't be made, there is no order with id: {}",
                line!(),
                order_id
            ),
        )))
    }
}

pub fn rollout_tx(
    user_id: &str,
    order_id: &str,
    conn: &mut r2d2::PooledConnection<RedisConnectionManager>,
) -> Result<(), Box<dyn std::error::Error>> {
    let tx_key = &format!("tx:user_id:{}:order_id:{}", user_id, order_id);
    let _ = redis::cmd("DEL").arg(tx_key).query(conn.deref_mut())?;
    Ok(())
}

pub fn commit_tx(
    user_id: &str,
    order_id: &str,
    conn: &mut r2d2::PooledConnection<RedisConnectionManager>,
    delete_order: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let order_key = &format!("user_id:{}:order_id:{}", user_id, order_id);
    let tx_key = &format!("tx:{}", order_key);

    if delete_order {
        let _ = redis::cmd("DEL").arg(order_key).query(conn.deref_mut())?;
    } else {
        let mut pipe = redis::pipe();
        let _ = pipe
            .cmd("MULTI")
            .cmd("DEL")
            .arg(order_key)
            .cmd("EVAL")
            .arg(&[EXEC_TX, "2", tx_key, order_key])
            .cmd("EXEC")
            .query(conn.deref_mut())?;
    }

    let _ = redis::cmd("DEL").arg(tx_key).query(conn.deref_mut())?;

    Ok(())
}
