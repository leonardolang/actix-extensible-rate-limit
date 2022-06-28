use actix_web::rt::time::Instant;

pub struct Value {
    pub ttl: Instant,
    pub count: u64,
}
