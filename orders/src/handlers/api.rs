use actix_web::{HttpRequest, HttpResponse};

pub fn get_orders(_req: HttpRequest) -> HttpResponse {
    HttpResponse::Ok().finish()
}

pub fn get_order_detailed(_req: HttpRequest) -> HttpResponse {
    HttpResponse::Ok().finish()
}
