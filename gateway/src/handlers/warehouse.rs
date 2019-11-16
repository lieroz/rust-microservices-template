use actix_web::HttpResponse;

pub fn get_goods() -> HttpResponse {
    // send to warehouse service api method
    HttpResponse::Ok().finish()
}

pub fn get_good_delailed() -> HttpResponse {
    // send to warehouse service api method
    HttpResponse::Ok().finish()
}
