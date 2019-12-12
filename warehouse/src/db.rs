use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use serde::Deserialize;
use std::io::{Error, ErrorKind};
use std::ops::DerefMut;

#[derive(Deserialize)]
struct CreateGood {
    id: u64,
    count: i64,
}

#[derive(Deserialize)]
pub struct CreateOrder {
    goods: Vec<CreateGood>,
}

impl CreateOrder {
    pub fn create(
        &self,
        user_id: &str,
        order_id: &str,
        conn: &mut r2d2::PooledConnection<RedisConnectionManager>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tx_key = &format!("tx:user_id:{}:order_id:{}", user_id, order_id);
        let tx_exists: i32 = redis::cmd("EXISTS").arg(tx_key).query(conn.deref_mut())?;

        if tx_exists == 1 {
            let mut pipe = redis::pipe();

            for good in &self.goods {
                pipe.cmd("HGET")
                    .arg(&[&format!("good_id:{}", good.id), "count"]);
            }

            let goods: Vec<redis::Value> = pipe.query(conn.deref_mut())?;

            if goods.is_empty() {
                return Err(Box::new(Error::new(
                    ErrorKind::Other,
                    format!("line:{}: There are no goods specified in order!", line!()),
                )));
            } else {
                pipe = redis::pipe();

                for (i, data) in goods.iter().enumerate() {
                    match data {
                        redis::Value::Data(data) => {
                            let count: i64 = std::str::from_utf8(&data)?.parse()?;
                            let good = &self.goods[i];

                            if count >= good.count {
                                pipe.cmd("HSET").arg(&[
                                    &format!("good_id:{}", good.id),
                                    "count",
                                    &(count - good.count).to_string(),
                                ]);
                            } else {
                                return Err(Box::new(Error::new(
                                    ErrorKind::Other,
                                    format!(
                                        "line:{}: Not enough good in warehouse with id: {}",
                                        line!(),
                                        self.goods[i].id
                                    ),
                                )));
                            }
                        }
                        redis::Value::Nil => {
                            return Err(Box::new(Error::new(
                                ErrorKind::Other,
                                format!(
                                    "line:{}: There is no good with id: {}",
                                    line!(),
                                    self.goods[i].id
                                ),
                            )));
                        }
                        value => {
                            return Err(Box::new(Error::new(
                                ErrorKind::Other,
                                format!(
                                    "line:{}: Redis returned invalid value: {:?}",
                                    line!(),
                                    value
                                ),
                            )));
                        }
                    }
                }

                let _ = pipe.query(conn.deref_mut())?;
            }
        } else {
            return Err(Box::new(Error::new(
                ErrorKind::Other,
                format!(
                    "line:{}: Transaction with id: {} doesn't exist",
                    line!(),
                    tx_key
                ),
            )));
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
        let order_key = &format!("user_id:{}:order_id:{}", user_id, order_id);
        let tx_key = &format!("tx:{}", order_key);
        let tx_exists: i32 = redis::cmd("EXISTS").arg(tx_key).query(conn.deref_mut())?;

        if tx_exists == 1 {
            let mut pipe = redis::pipe();

            for good in &self.goods {
                pipe.cmd("HGET")
                    .arg(&[&format!("good_id:{}", good.id), "count"]);
            }

            let goods: Vec<redis::Value> = pipe.query(conn.deref_mut())?;

            for (i, data) in goods.iter().enumerate() {
                match data {
                    redis::Value::Data(data) => {
                        let good = &self.goods[i];
                        let total: i64 = std::str::from_utf8(&data)?.parse()?;
                        let order_good_count = match redis::cmd("HGET")
                            .arg(&[order_key, &format!("good_id:{}", good.id)])
                            .query(conn.deref_mut())?
                        {
                            redis::Value::Data(data) => {
                                std::str::from_utf8(&data)?.parse::<i64>()?
                            }
                            redis::Value::Nil => 0,
                            value => {
                                return Err(Box::new(Error::new(
                                    ErrorKind::Other,
                                    format!(
                                        "line:{}: Redis returned invalid value: {:?}",
                                        line!(),
                                        value
                                    ),
                                )));
                            }
                        };

                        match &good.operation[..] {
                            "update" => {
                                let diff = order_good_count - good.count;
                                if total - diff >= 0 {
                                    pipe.cmd("HINCRBY").arg(&[
                                        &format!("good_id:{}", good.id),
                                        "count",
                                        &diff.to_string(),
                                    ]);
                                } else {
                                    return Err(Box::new(Error::new(
                                        ErrorKind::Other,
                                        format!(
                                            "line:{}: Not enough good in warehouse with id: {}",
                                            line!(),
                                            self.goods[i].id
                                        ),
                                    )));
                                }
                            }
                            "delete" => {
                                pipe.cmd("HINCRBY").arg(&[
                                    &format!("good_id:{}", good.id),
                                    "count",
                                    &order_good_count.to_string(),
                                ]);
                            }
                            _ => {
                                return Err(Box::new(Error::new(
                                    ErrorKind::Other,
                                    format!(
                                        "line:{}: Unknown operation: {}",
                                        line!(),
                                        good.operation
                                    ),
                                )));
                            }
                        }
                    }
                    redis::Value::Nil => {
                        return Err(Box::new(Error::new(
                            ErrorKind::Other,
                            format!(
                                "line:{}: There is no good with id: {}",
                                line!(),
                                self.goods[i].id
                            ),
                        )));
                    }
                    value => {
                        return Err(Box::new(Error::new(
                            ErrorKind::Other,
                            format!(
                                "line:{}: Redis returned invalid value: {:?}",
                                line!(),
                                value
                            ),
                        )));
                    }
                }
            }

            let _ = pipe.query(conn.deref_mut())?;
        } else {
            return Err(Box::new(Error::new(
                ErrorKind::Other,
                format!(
                    "line:{}: Transaction with id: {} doesn't exist",
                    line!(),
                    tx_key
                ),
            )));
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
