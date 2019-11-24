use actix_web::{web, HttpRequest, HttpResponse};
use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use serde_json::map::Map;
use serde_json::value::Value;
use std::ops::DerefMut;

pub fn get_orders(
    req: HttpRequest,
    db: web::Data<r2d2::Pool<RedisConnectionManager>>,
) -> HttpResponse {
    let query = qstring::QString::from(req.query_string());
    let limit = match query.get("limit") {
        Some(limit) if limit.parse::<i64>().unwrap() > 0 => limit,
        _ => "100",
    };
    let mut conn = db.get().unwrap();

    let result = redis::cmd("SCAN")
        .cursor_arg(0)
        .arg(&["MATCH", "user_id:{}:order_id:*", "COUNT", &limit])
        .query::<Vec<redis::Value>>(conn.deref_mut())
        .unwrap();

    let mut pipe = redis::pipe();

    for x in result {
        match x {
            redis::Value::Data(cur) => println!(
                "{:?}",
                std::str::from_utf8(&cur).unwrap().parse::<u64>().unwrap()
            ),
            redis::Value::Bulk(data) => data.iter().for_each(|x| {
                if let redis::Value::Data(s) = x {
                    pipe.cmd("GET").arg(std::str::from_utf8(&s).unwrap());
                }
            }),
            _ => (),
        }
    }

    let items: redis::Value = pipe.query(conn.deref_mut()).unwrap();

    if let redis::Value::Bulk(data) = items {
        for x in data {
            if let redis::Value::Data(s) = x {
                println!(
                    "{:?}",
                    std::str::from_utf8(&s).unwrap().parse::<u64>().unwrap()
                );
            }
        }
    }

    HttpResponse::Ok().finish()
}

// FIXME: OMG!!! plz change redis for posrgresql and diesel...
pub fn get_order_detailed(
    params: web::Path<(String, String)>,
    db: web::Data<r2d2::Pool<RedisConnectionManager>>,
) -> HttpResponse {
    let mut conn = db.get().unwrap();
    let redis_key = &format!("user_id:{}:order_id:{}", params.0, params.1);

    match redis::cmd("HGETALL").arg(redis_key).query(conn.deref_mut()) {
        Ok(redis::Value::Bulk(ref bulk)) => {
            if bulk.is_empty() {
                warn!("Warning: order with id: {} wasn't found", redis_key);
                HttpResponse::NotFound().finish()
            } else {
                let mut json: Map<String, Value> = Map::new();
                let mut goods: Vec<Value> = vec![];
                let mut i = 0;

                while i < bulk.len() {
                    if let redis::Value::Data(data) = &bulk[i] {
                        let key = String::from_utf8(data.to_vec()).unwrap();

                        if let redis::Value::Data(data) = &bulk[i + 1] {
                            let value = String::from_utf8(data.to_vec()).unwrap();

                            if key.contains("good_id:") {
                                let mut good: Map<String, Value> = Map::new();
                                let splits: Vec<&str> = key.split(':').collect();
                                let order_id = splits[1].to_string().parse::<u64>().unwrap();
                                let value = value.parse::<u64>().unwrap();

                                good.insert(
                                    splits[0].to_string(),
                                    Value::Number(serde_json::Number::from(order_id)),
                                );
                                good.insert(
                                    "count".to_string(),
                                    Value::Number(serde_json::Number::from(value)),
                                );
                                goods.push(Value::Object(good));
                            } else {
                                json.insert(key, Value::String(value));
                            }
                        }
                    }

                    i += 2;
                }

                json.insert("goods".to_string(), Value::Array(goods));
                HttpResponse::Ok().json(json)
            }
        }
        Ok(result) => {
            error!("Error: redis returned invalid answer: {:?}", result);
            HttpResponse::InternalServerError().finish()
        }
        Err(error) => {
            error!("Error: redis error: {}", error);
            HttpResponse::InternalServerError().finish()
        }
    }
}
