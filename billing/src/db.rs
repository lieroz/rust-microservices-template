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
    ) {
        let redis_key = &format!("user_id:{}:order_id:{}", user_id, order_id);

        let result = redis::cmd("HMGET")
            .arg(&[redis_key, "validated", "status"])
            .query(conn.deref_mut());

        match result {
            Ok(redis::Value::Bulk(bulk)) => {
                if let redis::Value::Data(data) = &bulk[0] {
                    let validated = std::str::from_utf8(&data).unwrap();

                    if validated == "true" {
                        if let redis::Value::Data(data) = &bulk[1] {
                            let status = String::from_utf8(data.to_vec()).unwrap();

                            if status == "payed" {
                                debug!("Order: {} is already payed", order_id);
                            } else if status == "created" {
                                let result = redis::cmd("HSET")
                                    .arg(redis_key)
                                    .arg("status")
                                    .arg("payed")
                                    .query(conn.deref_mut())
                                    .unwrap();

                                match result {
                                    redis::Value::Int(status) if status == 0 => {
                                        info!("Billing created successfully!");
                                    }
                                    value => error!(
                                        "Error: redis server returned invalid value: {:?}",
                                        value
                                    ),
                                }
                            } else {
                                error!("Order: {} has invalid status: {}", order_id, status);
                            }
                        } else {
                            error!("Redis return invalid 'status' answer on HMGET cmd");
                        }
                    } else {
                        error!("Order with id: {} is not validated!", order_id);
                    }
                } else {
                    error!("Redis returned invalid 'validated' answer on HMGET cmd");
                }
            }
            Ok(value) => error!(
                "Error redis returned invalid value on HMGET cmd: {:?}",
                value
            ),
            Err(e) => error!("Error happened on redis HMGET cmd: {}", e),
        }
    }
}
