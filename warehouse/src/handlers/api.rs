use actix_web::{web, HttpRequest, HttpResponse};
use qstring::QString;
use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use std::ops::DerefMut;

pub fn get_goods(
    req: HttpRequest,
    db: web::Data<r2d2::Pool<RedisConnectionManager>>,
) -> HttpResponse {
    let query = QString::from(req.query_string());
    let limit = match query.get("limit") {
        Some(limit) if limit.parse::<i64>().unwrap() > 0 => limit,
        _ => "100",
    };
    let mut conn = db.get().unwrap();

    let result = redis::cmd("SCAN")
        .cursor_arg(0)
        .arg(&["MATCH", "good:*", "COUNT", &limit])
        .query::<Vec<redis::Value>>(conn.deref_mut())
        .unwrap();

    for x in result.iter() {
        match x {
            redis::Value::Data(cur) => println!(
                "{:?}",
                std::str::from_utf8(&cur).unwrap().parse::<u64>().unwrap()
            ),
            redis::Value::Bulk(data) => data.iter().for_each(|x| {
                if let redis::Value::Data(s) = x {
                    println!("{:?}", std::str::from_utf8(&s).unwrap());
                }
            }),
            _ => (),
        }
    }

    HttpResponse::Ok().finish()
}
