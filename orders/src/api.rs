use actix_web::{web, HttpRequest, HttpResponse};
use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use std::ops::DerefMut;

pub fn get_orders(
    _req: HttpRequest,
    _db: web::Data<r2d2::Pool<RedisConnectionManager>>,
) -> HttpResponse {
    HttpResponse::Ok().finish()
}

pub fn get_order_detailed(
    _req: HttpRequest,
    _db: web::Data<r2d2::Pool<RedisConnectionManager>>,
) -> HttpResponse {
    HttpResponse::Ok().finish()
}
