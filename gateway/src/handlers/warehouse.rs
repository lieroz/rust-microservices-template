use actix_web::{HttpRequest, HttpResponse};

pub fn get_goods(req: HttpRequest) -> HttpResponse {
    HttpResponse::Ok().finish()
}

pub fn get_good_delailed(req: HttpRequest) -> HttpResponse {
    HttpResponse::Ok().finish()
}
