use actix_web::{web, HttpRequest, HttpResponse};
use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use std::ops::DerefMut;

pub fn get_goods(
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
        .arg(&["MATCH", "item:*", "COUNT", &limit])
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

pub fn get_good(req: HttpRequest) -> HttpResponse {
    HttpResponse::Ok().finish()
}
