use actix_web::{web, HttpResponse};

use crate::handlers::api::*;

pub fn config_app(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("")
            .service(web::resource("/liveness_probe").route(web::get().to(|| HttpResponse::Ok())))
            .service(web::resource("/goods").route(web::get().to(get_goods))),
    );
}
