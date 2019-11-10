use actix_web::{HttpRequest, HttpResponse};

pub fn get_orders(req: HttpRequest) -> HttpResponse {
    HttpResponse::Ok().finish()
}

pub fn get_order_detailed(req: HttpRequest) -> HttpResponse {
    HttpResponse::Ok().finish()
}
