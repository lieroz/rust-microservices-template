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

        if let redis::Value::Bulk(bulk) = redis::cmd("HGETALL")
            .arg(redis_key)
            .query(conn.deref_mut())
            .unwrap()
        {
            if bulk.is_empty() {
                error!(
                    "Billing can't be made, order with id: {} doesn't exist",
                    order_id
                );
            } else {
                for i in 0..bulk.len() {
                    if let redis::Value::Data(data) = &bulk[i] {
                        let result = String::from_utf8(data.to_vec()).unwrap();

                        if result == "status" {
                            if let redis::Value::Data(data) = &bulk[i + 1] {
                                let status = String::from_utf8(data.to_vec()).unwrap();

                                if status == "payed" {
                                    debug!("Order: {} is already payed", order_id);
                                    return;
                                } else if status == "created" {
                                    break;
                                } else {
                                    error!("Order: {} has invalid status: {}", order_id, status);
                                    return;
                                }
                            }
                        }
                    }
                }

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
                    value => error!("Error: redis server returned invalid value: {:?}", value),
                }
            }
        } else {
            error!("Error: redis returned invalid answer");
        }
    }
}
