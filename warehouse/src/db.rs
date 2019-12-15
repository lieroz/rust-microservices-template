use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use serde::Deserialize;
use std::collections::HashMap;
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
        conn: &mut r2d2::PooledConnection<RedisConnectionManager>,
    ) -> Result<(), Box<dyn std::error::Error>> {
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

        Ok(())
    }
}

#[derive(Deserialize)]
struct UpdateGood {
    id: u64,
    #[serde(default)]
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
        conn: &mut r2d2::PooledConnection<RedisConnectionManager>,
    ) -> Result<(), Box<dyn std::error::Error>> {
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

                    match &good.operation[..] {
                        "update" => {
                            if total + good.count >= 0 {
                                pipe.cmd("HINCRBY").arg(&[
                                    &format!("good_id:{}", good.id),
                                    "count",
                                    &good.count.to_string(),
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
                                &good.count.to_string(),
                            ]);
                        }
                        _ => {
                            return Err(Box::new(Error::new(
                                ErrorKind::Other,
                                format!("line:{}: Unknown operation: {}", line!(), good.operation),
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

        Ok(())
    }
}

pub fn delete_order(
    goods: HashMap<String, u64>,
    conn: &mut r2d2::PooledConnection<RedisConnectionManager>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut pipe = redis::pipe();

    for (k, v) in goods {
        pipe.cmd("HINCRBY").arg(&[&k, "count", &v.to_string()]);
    }

    let _ = pipe.query(conn.deref_mut())?;

    Ok(())
}
