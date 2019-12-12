use actix_web::{web, HttpRequest, HttpResponse};
use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use serde_json::map::Map;
use serde_json::value::Value;
use std::ops::DerefMut;

pub fn get_goods(
    req: HttpRequest,
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

    let key_matcher = &format!("good_id:*");

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

    if let redis::Value::Bulk(bulk) = items {
        let mut goods: Vec<Map<String, Value>> = vec![];

        for (i, x) in bulk.iter().enumerate() {
            if let redis::Value::Bulk(bulk) = x {
                let mut good: Map<String, Value> = Map::new();
                good.insert(
                    "id".to_string(),
                    Value::Number(serde_json::Number::from(keys[i])),
                );

                let mut j = 0;

                while j < bulk.len() {
                    if let redis::Value::Data(data) = &bulk[j] {
                        let key = std::str::from_utf8(data).unwrap();

                        if let redis::Value::Data(data) = &bulk[j + 1] {
                            let value: u64 = std::str::from_utf8(data).unwrap().parse().unwrap();

                            good.insert(
                                key.to_string(),
                                Value::Number(serde_json::Number::from(value)),
                            );
                        }
                    }

                    j += 2;
                }

                goods.push(good);
            }
        }

        if goods.is_empty() {
            HttpResponse::NotFound().finish()
        } else {
            HttpResponse::Ok().json(goods)
        }
    } else {
        HttpResponse::NotFound().finish()
    }
}

pub fn get_good(
    good_id: web::Path<(String)>,
    db: web::Data<r2d2::Pool<RedisConnectionManager>>,
) -> HttpResponse {
    let mut conn = match db.get() {
        Ok(conn) => conn,
        Err(e) => {
            error!("{}:Couldn't get connection to database: {}", line!(), e);
            return HttpResponse::InternalServerError().finish();
        }
    };

    let result = redis::cmd("HGETALL")
        .arg(&[format!("good_id:{}", good_id)])
        .query(conn.deref_mut());

    let mut good: Map<String, Value> = Map::new();

    match result {
        Ok(x) => {
            if let redis::Value::Bulk(bulk) = x {
                if !bulk.is_empty() {
                    good.insert(
                        "id".to_string(),
                        Value::Number(serde_json::Number::from(good_id.parse::<u64>().unwrap())),
                    );

                    let mut i = 0;

                    while i < bulk.len() {
                        if let redis::Value::Data(data) = &bulk[i] {
                            let key = std::str::from_utf8(data).unwrap();

                            if let redis::Value::Data(data) = &bulk[i + 1] {
                                let value: u64 =
                                    std::str::from_utf8(data).unwrap().parse().unwrap();

                                good.insert(
                                    key.to_string(),
                                    Value::Number(serde_json::Number::from(value)),
                                );
                            }
                        }

                        i += 2;
                    }
                    HttpResponse::Ok().json(good)
                } else {
                    HttpResponse::NotFound().finish()
                }
            } else {
                HttpResponse::InternalServerError().finish()
            }
        }
        Err(e) => {
            error!("Error: {}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}
