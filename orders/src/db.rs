use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use serde::Deserialize;
use std::ops::DerefMut;

const TX_EXPIRE: &str = "60";

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
        let redis_key = &format!("tx:user_id:{}:order_id:{}", order_id, user_id);

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

        pipe.cmd("EXPIRE").arg(&[redis_key, TX_EXPIRE]);
        let _ = pipe.cmd("EXEC").query(conn.deref_mut())?;

        Ok(order_id)
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

const EXEC_TX: &str = r#"
    if redis.call('EXISTS', KEYS[2]) == 1 then
        return { err = 'Transaction '..KEYS[2]..' already exists'}
    end
    local hash = redis.call('HGETALL', KEYS[1]);
    if #hash == 0 then
        return { err = 'The key '..KEYS[1]..' does not exist' }
    end
    return redis.call('HMSET', KEYS[2], unpack(hash))"#;

impl UpdateOrder {
    pub fn update(
        &self,
        user_id: &str,
        order_id: &str,
        conn: &mut r2d2::PooledConnection<RedisConnectionManager>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let order_key = &format!("user_id:{}:order_id:{}", user_id, order_id);
        let tx_key = &format!("tx:{}", order_key);

        let status: String = redis::cmd("EVAL")
            .arg(&[EXEC_TX, "2", order_key, tx_key])
            .query(conn.deref_mut())?;

        if status == "OK" {
            let _ = redis::cmd("EXPIRE")
                .arg(&[tx_key, TX_EXPIRE])
                .query(conn.deref_mut())?;

            let mut pipe = redis::pipe();

            for good in &self.goods {
                let good_id = &format!("good_id:{}", good.id);

                match &good.operation[..] {
                    "update" => {
                        pipe.cmd("HSET")
                            .arg(&[tx_key, good_id, &good.count.to_string()]);
                    }
                    "delete" => {
                        pipe.cmd("HDEL").arg(&[tx_key, good_id]);
                    }
                    _ => {
                        error!("line:{}: Unknown operation: {}", line!(), good.operation);
                    }
                }

                let _ = pipe.query(conn.deref_mut())?;
            }
        } else {
            error!(
                "line:{}: Redis returned invalid status: {}",
                line!(),
                status
            );
        }

        Ok(())
    }
}

pub fn delete_order(
    user_id: &str,
    order_id: &str,
    conn: &mut r2d2::PooledConnection<RedisConnectionManager>,
) -> Result<(), Box<dyn std::error::Error>> {
    let order_key = &format!("user_id:{}:order_id:{}", user_id, order_id);
    let tx_key = &format!("tx:{}", order_key);

    let status: String = redis::cmd("EVAL")
        .arg(&[EXEC_TX, "2", order_key, tx_key])
        .query(conn.deref_mut())?;

    if status == "OK" {
        let _ = redis::cmd("EXPIRE")
            .arg(&[tx_key, TX_EXPIRE])
            .query(conn.deref_mut())?;
    } else {
        error!(
            "line:{}: Redis returned invalid status: {}",
            line!(),
            status
        );
    }

    Ok(())
}
