use actix_web::{web, HttpRequest, HttpResponse};
use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use serde_json::map::Map;
use serde_json::value::Value;
use std::ops::DerefMut;

// FIXME: OMG!!! plz change redis for posrgresql and diesel...
fn parse_redis_answer(bulk: Vec<redis::Value>) -> Option<Map<String, Value>> {
    let mut json: Map<String, Value> = Map::new();
    let mut goods: Vec<Value> = vec![];
    let mut i = 0;

    while i < bulk.len() {
        if let redis::Value::Data(data) = &bulk[i] {
            let key = match String::from_utf8(data.to_vec()) {
                Ok(s) => s,
                Err(e) => {
                    error!("{}:Couldn't deserialize redis answer: {}", line!(), e);
                    return None;
                }
            };

            if let redis::Value::Data(data) = &bulk[i + 1] {
                let value = match String::from_utf8(data.to_vec()) {
                    Ok(s) => s,
                    Err(e) => {
                        error!("{}:Couldn't deserialize redis answer: {}", line!(), e);
                        return None;
                    }
                };

                if key.contains("good_id:") {
                    let mut good: Map<String, Value> = Map::new();
                    let splits: Vec<&str> = key.split(':').collect();

                    let order_id = match splits[1].to_string().parse::<u64>() {
                        Ok(id) => id,
                        Err(e) => {
                            error!("{}:Couldn't convert to number: {}", line!(), e);
                            return None;
                        }
                    };

                    let value = match value.parse::<u64>() {
                        Ok(v) => v,
                        Err(e) => {
                            error!("{}:Couldn't convert to number: {}", line!(), e);
                            return None;
                        }
                    };

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
    Some(json)
}

pub fn get_orders(
    req: HttpRequest,
    user_id: web::Path<(String)>,
    db: web::Data<r2d2::Pool<RedisConnectionManager>>,
) -> HttpResponse {
    let query = qstring::QString::from(req.query_string());
    let limit = match query.get("limit") {
        Some(limit) if limit.parse::<i64>().unwrap_or(100) > 0 => limit,
        _ => "100",
    };

    let mut conn = match db.get() {
        Ok(conn) => conn,
        Err(e) => {
            error!("{}:Couldn't get connection to database: {}", line!(), e);
            return HttpResponse::InternalServerError().finish();
        }
    };

    let key_matcher = &format!("user_id:{}:order_id:*", user_id);

    let result = match redis::cmd("SCAN")
        .cursor_arg(0)
        .arg(&["MATCH", key_matcher, "COUNT", &limit])
        .query::<Vec<redis::Value>>(conn.deref_mut())
    {
        Ok(r) => r,
        Err(e) => {
            error!(
                "{}:Error happened on cmd to redis 'SCAN 0 MATCH {} COUNT ': {}",
                key_matcher, limit, e
            );
            return HttpResponse::InternalServerError().finish();
        }
    };

    let mut pipe = redis::pipe();
    let mut keys = Vec::new();

    for x in result {
        match x {
            redis::Value::Bulk(data) => {
                for x in data {
                    if let redis::Value::Data(s) = x {
                        let key = match String::from_utf8(s) {
                            Ok(s) => s,
                            Err(e) => {
                                error!("{}:Couldn't deserialize redis answer: {}", line!(), e);
                                return HttpResponse::InternalServerError().finish();
                            }
                        };

                        pipe.cmd("HGETALL").arg(&key);

                        let values: Vec<&str> = key.split(":").collect();

                        if let Some(key) = values.last() {
                            match key.parse::<u32>() {
                                Ok(key) => {
                                    keys.push(key);
                                }
                                Err(e) => {
                                    error!("{}:Couldn't convert string to number: {}", line!(), e);
                                    return HttpResponse::InternalServerError().finish();
                                }
                            }
                        }
                    }
                }
            }
            _ => (),
        }
    }

    let items: redis::Value = match pipe.query(conn.deref_mut()) {
        Ok(x) => x,
        Err(e) => {
            error!("{}:Couldn't execute redis pipeline request: {}", line!(), e);
            return HttpResponse::InternalServerError().finish();
        }
    };

    if let redis::Value::Bulk(data) = items {
        let mut result: Vec<Map<String, Value>> = Vec::new();

        for x in data {
            if let redis::Value::Bulk(bulk) = x {
                if let Some(mut json) = parse_redis_answer(bulk) {
                    if json["status"] != "deleted" {
                        json.insert(
                            "order_id".to_string(),
                            Value::Number(serde_json::Number::from(keys.remove(0))),
                        );
                        result.push(json);
                    }
                } else {
                    return HttpResponse::InternalServerError().finish();
                }
            }
        }

        if result.is_empty() {
            HttpResponse::NotFound().json(result)
        } else {
            HttpResponse::Ok().json(result)
        }
    } else {
        HttpResponse::NotFound().finish()
    }
}

pub fn get_order(
    params: web::Path<(String, String)>,
    db: web::Data<r2d2::Pool<RedisConnectionManager>>,
) -> HttpResponse {
    let mut conn = match db.get() {
        Ok(conn) => conn,
        Err(e) => {
            error!("{}:Couldn't get connection to database: {}", line!(), e);
            return HttpResponse::InternalServerError().finish();
        }
    };

    let redis_key = &format!("user_id:{}:order_id:{}", params.0, params.1);

    match redis::cmd("HGETALL").arg(redis_key).query(conn.deref_mut()) {
        Ok(redis::Value::Bulk(bulk)) => {
            if bulk.is_empty() {
                error!("{}:Order with id: {} wasn't found", line!(), redis_key);
                HttpResponse::NotFound().finish()
            } else {
                if let Some(json) = parse_redis_answer(bulk) {
                    if json["status"] != "deleted" {
                        HttpResponse::Ok().json(json)
                    } else {
                        HttpResponse::NotFound().finish()
                    }
                } else {
                    HttpResponse::InternalServerError().finish()
                }
            }
        }
        Ok(result) => {
            error!("{}:Redis returned invalid answer: {:?}", line!(), result);
            HttpResponse::InternalServerError().finish()
        }
        Err(e) => {
            error!("{}:Redis error: {}", line!(), e);
            HttpResponse::InternalServerError().finish()
        }
    }
}
