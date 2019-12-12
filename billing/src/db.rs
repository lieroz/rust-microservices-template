use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use serde::Deserialize;
use std::io::{Error, ErrorKind};
use std::ops::DerefMut;

#[derive(Deserialize)]
pub struct CreateBilling {
    id: u64,
}

impl CreateBilling {
    pub fn create(
        &self,
        user_id: &str,
        order_id: &str,
        conn: &mut r2d2::PooledConnection<RedisConnectionManager>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let order_key = &format!("user_id:{}:order_id:{}", user_id, order_id);
        let tx_key = &format!("tx:{}", order_key);

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
    }
}
