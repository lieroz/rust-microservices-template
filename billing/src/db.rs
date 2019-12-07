use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use serde::Deserialize;
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
        let redis_key = &format!("user_id:{}:order_id:{}", user_id, order_id);

        let result = redis::cmd("HMGET")
            .arg(&[redis_key, "validated", "status"])
            .query(conn.deref_mut())?;

        if let redis::Value::Bulk(bulk) = result {
            if let redis::Value::Data(data) = &bulk[0] {
                let validated = std::str::from_utf8(&data)?;

                if validated == "true" {
                    if let redis::Value::Data(data) = &bulk[1] {
                        let status = String::from_utf8(data.to_vec())?;

                        if status == "payed" {
                            debug!("Order: {} is already payed", order_id);
                        } else if status == "created" {
                            let _ = redis::cmd("HSET")
                                .arg(redis_key)
                                .arg("status")
                                .arg("payed")
                                .query(conn.deref_mut())?;
                        }
                    }
                } else {
                    error!("Order with id: {} is not validated!", order_id);
                }
            }
        }

        Ok(())
    }
}
