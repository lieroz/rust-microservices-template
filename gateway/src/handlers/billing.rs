use actix_web::{HttpRequest, HttpResponse};

pub fn make_billing(req: HttpRequest) -> HttpResponse {
    HttpResponse::Ok().finish()
}
