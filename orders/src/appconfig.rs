use actix_web::{web, HttpResponse};

use crate::api::*;

pub fn config_app(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("")
            .service(
                web::scope("/probe")
                    .service(web::resource("/liveness").route(web::get().to(|| HttpResponse::Ok())))
                    .service(
                        web::resource("/readiness").route(web::get().to(|| HttpResponse::Ok())),
                    )
                    .service(web::resource("/startup").route(web::get().to(|| HttpResponse::Ok()))),
            )
            .service(
                web::scope("/user/{user_id}")
                    .service(web::resource("/orders").route(web::get().to(get_orders)))
                    .service(
                        web::resource("/order/{order_id}").route(web::get().to(get_order_detailed)),
                    ),
            ),
    );
}
