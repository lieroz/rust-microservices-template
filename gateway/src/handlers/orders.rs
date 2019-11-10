use actix_web::{HttpRequest, HttpResponse};

pub fn get_orders(req: HttpRequest) -> HttpResponse {
    // send to api method
    HttpResponse::Ok().finish()
}

pub fn create_order(req: HttpRequest) -> HttpResponse {
    // add mpsc send to kafka thread
    HttpResponse::Ok().finish()
}

pub fn get_order_detailed(req: HttpRequest) -> HttpResponse {
    // send to api method
    HttpResponse::Ok().finish()
}

pub fn update_order(req: HttpRequest) -> HttpResponse {
    // add mpsc send to kafka thread
    HttpResponse::Ok().finish()
}

pub fn add_good_to_order(req: HttpRequest) -> HttpResponse {
    // add mpsc send to kafka thread
    HttpResponse::Ok().finish()
}

pub fn delete_good_from_order(req: HttpRequest) -> HttpResponse {
    // add mpsc send to kafka thread
    HttpResponse::Ok().finish()
}
