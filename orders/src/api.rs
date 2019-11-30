use actix_web::{web, HttpRequest, HttpResponse};
use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use serde_json::map::Map;
use serde_json::value::Value;
use std::ops::DerefMut;

// FIXME: OMG!!! plz change redis for posrgresql and diesel...
fn parse_redis_answer(bulk: Vec<redis::Value>) -> Map<String, Value> {
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
    json
}

pub fn get_orders(
    req: HttpRequest,
    user_id: web::Path<(String)>,
    db: web::Data<r2d2::Pool<RedisConnectionManager>>,
) -> HttpResponse {
    let query = qstring::QString::from(req.query_string());
    let limit = match query.get("limit") {
        Some(limit) if limit.parse::<i64>().unwrap() > 0 => limit,
        _ => "100",
    };
    let mut conn = db.get().unwrap();
    let key_matcher = &format!("user_id:{}:order_id:*", user_id);

    let result = redis::cmd("SCAN")
        .cursor_arg(0)
        .arg(&["MATCH", key_matcher, "COUNT", &limit])
        .query::<Vec<redis::Value>>(conn.deref_mut())
        .unwrap();

    let mut pipe = redis::pipe();
    let mut keys = Vec::new();

    for x in result {
        match x {
            redis::Value::Bulk(data) => {
                for x in data {
                    if let redis::Value::Data(s) = x {
                        let key = String::from_utf8(s).unwrap();
                        pipe.cmd("HGETALL").arg(&key);

                        let values: Vec<&str> = key.split(":").collect();
                        keys.push(values.last().unwrap().parse::<u32>().unwrap());
                    }
                }
            }
            _ => (),
        }
    }

    let items: redis::Value = pipe.query(conn.deref_mut()).unwrap();

    if let redis::Value::Bulk(data) = items {
        let mut result: Vec<Map<String, Value>> = Vec::new();

        for x in data {
            if let redis::Value::Bulk(bulk) = x {
                let mut json = parse_redis_answer(bulk);
                json.insert(
                    "order_id".to_string(),
                    Value::Number(serde_json::Number::from(keys.remove(0))),
                );
                result.push(json);
            }
        }

        HttpResponse::Ok().json(result)
    } else {
        HttpResponse::NotFound().finish()
    }
}

pub fn get_order(
    params: web::Path<(String, String)>,
    db: web::Data<r2d2::Pool<RedisConnectionManager>>,
) -> HttpResponse {
    let mut conn = db.get().unwrap();
    let redis_key = &format!("user_id:{}:order_id:{}", params.0, params.1);

    match redis::cmd("HGETALL").arg(redis_key).query(conn.deref_mut()) {
        Ok(redis::Value::Bulk(bulk)) => {
            if bulk.is_empty() {
                warn!("Warning: order with id: {} wasn't found", redis_key);
                HttpResponse::NotFound().finish()
            } else {
                let json = parse_redis_answer(bulk);
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
