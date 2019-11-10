use actix_web::{web, HttpResponse};

use crate::handlers::billing::*;
use crate::handlers::orders::*;
use crate::handlers::warehouse::*;

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
                web::scope("/goods")
                    .service(web::resource("").route(web::get().to(get_goods)))
                    .service(web::resource("/{id}").route(web::get().to(get_good_delailed))),
            )
            .service(
                web::scope("/user/{user_id}")
                    .service(web::resource("/orders").route(web::get().to(get_orders)))
                    .service(web::resource("/order").route(web::post().to(create_order)))
                    .service(
                        web::resource("/order/{id}")
                            .route(web::get().to(get_order_detailed))
                            .route(web::put().to(update_order)),
                    )
                    .service(
                        web::resource("/order/{order_id}/good/{good_id}")
                            .route(web::post().to(add_good_to_order))
                            .route(web::delete().to(delete_good_from_order)),
                    )
                    .service(
                        web::resource("/order/{id}/billing").route(web::post().to(make_billing)),
                    ),
            ),
    );
}
