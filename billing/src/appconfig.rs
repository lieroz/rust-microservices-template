use actix_web::{web, HttpResponse};

pub fn config_app(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("").service(
            web::scope("/probe")
                .service(web::resource("/liveness").route(web::get().to(|| HttpResponse::Ok())))
                .service(web::resource("/readiness").route(web::get().to(|| HttpResponse::Ok())))
                .service(web::resource("/startup").route(web::get().to(|| HttpResponse::Ok()))),
        ),
    );
}
