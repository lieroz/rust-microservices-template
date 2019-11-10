use actix_web::{HttpRequest, HttpResponse};

pub fn make_billing(req: HttpRequest) -> HttpResponse {
    // add mpsc send to kafka thread
    HttpResponse::Ok().finish()
}
