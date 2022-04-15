use crate::backend::SimpleInput;
use actix_web::dev::ServiceRequest;
use std::future::{ready, Ready};
use std::net::{IpAddr, Ipv6Addr};
use std::time::Duration;

pub type CustomFn = Box<dyn Fn(&ServiceRequest) -> Result<String, actix_web::Error>>;

/// Utility to create a input function that produces a [SimpleInput].
///
/// You should take care to ensure that you are producing unique keys per backend.
///
/// This will not be of any use if you want to use dynamic interval/request policies
/// or perform an asynchronous option; you should instead write your own input function.
pub struct SimpleInputFunctionBuilder {
    interval: Duration,
    max_requests: u64,
    real_ip_key: bool,
    peer_ip_key: bool,
    path_key: bool,
    custom_key: Option<String>,
    custom_fn: Option<CustomFn>,
}

impl SimpleInputFunctionBuilder {
    pub fn new(interval: Duration, max_requests: u64) -> Self {
        Self {
            interval,
            max_requests,
            real_ip_key: false,
            peer_ip_key: false,
            path_key: false,
            custom_key: None,
            custom_fn: None,
        }
    }

    /// Adds the client's real IP to the rate limiting key.
    ///
    /// # Security
    ///
    /// This calls
    /// [ConnectionInfo::realip_remote_addr()](actix_web::dev::ConnectionInfo::realip_remote_addr)
    /// internally which is only suitable for Actix applications deployed behind a proxy that you
    /// control.
    ///
    /// # IPv6
    ///
    /// IPv6 addresses will be grouped into a single key per /64
    pub fn real_ip_key(mut self) -> Self {
        self.real_ip_key = true;
        self
    }

    /// Adds the connection peer IP to the rate limiting key.
    ///
    /// This is suitable when clients connect directly to the Actix application.
    ///
    /// # IPv6
    ///
    /// IPv6 addresses will be grouped into a single key per /64
    pub fn peer_ip_key(mut self) -> Self {
        self.peer_ip_key = true;
        self
    }

    /// Add the request path to the rate limiting key
    pub fn path_key(mut self) -> Self {
        self.path_key = true;
        self
    }

    /// Add a custom component to the rate limiting key
    pub fn custom_key(mut self, key: &str) -> Self {
        self.custom_key = Some(key.to_owned());
        self
    }

    /// Dynamically add a custom component to the rate limiting key
    pub fn custom_fn<F>(mut self, f: F) -> Self
    where
        F: Fn(&ServiceRequest) -> Result<String, actix_web::Error> + 'static,
    {
        self.custom_fn = Some(Box::new(f));
        self
    }

    pub fn build(
        self,
    ) -> impl Fn(&ServiceRequest) -> Ready<Result<SimpleInput, actix_web::Error>> + 'static {
        move |req| {
            ready((|| {
                let mut components = Vec::new();
                let info = req.connection_info();
                if let Some(custom) = &self.custom_key {
                    components.push(custom.clone());
                }
                if self.real_ip_key {
                    components.push(ip_key(info.realip_remote_addr().unwrap()))
                }
                if self.peer_ip_key {
                    components.push(ip_key(info.peer_addr().unwrap()))
                }
                if self.path_key {
                    components.push(req.path().to_owned());
                }
                if let Some(f) = &self.custom_fn {
                    components.push(f(req)?)
                }
                let key = components.join("-");

                Ok(SimpleInput {
                    interval: self.interval,
                    max_requests: self.max_requests,
                    key,
                })
            })())
        }
    }
}

// Groups IPv6 addresses together, see:
// https://adam-p.ca/blog/2022/02/ipv6-rate-limiting/
// https://support.cloudflare.com/hc/en-us/articles/115001635128-Configuring-Cloudflare-Rate-Limiting
fn ip_key(ip_str: &str) -> String {
    let ip = ip_str
        .parse::<IpAddr>()
        .expect("Unable to parse remote IP address - proxy misconfiguration?");
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4() {
                return v4.to_string();
            }
            let zeroes = [0u16; 4];
            let concat = [&v6.segments()[0..4], &zeroes].concat();
            let concat: [u16; 8] = concat.try_into().unwrap();
            let subnet = Ipv6Addr::from(concat);
            format!("{}/64", subnet)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::memory::InMemoryBackend;
    use crate::RateLimiter;
    use actix_web::App;
    use std::time::Duration;

    #[actix_web::test]
    async fn test_use_with_middleware() {
        // Check that all the type signatures work together
        let backend = InMemoryBackend::builder().build();
        let input_fn = SimpleInputFunctionBuilder::new(Duration::from_secs(60), 60).build();
        let limiter = RateLimiter::builder(backend, input_fn).build();
        actix_web::test::init_service(App::new().wrap(limiter)).await;
    }

    #[test]
    fn test_ip_key() {
        // Check that IPv4 addresses are preserved
        assert_eq!(ip_key("142.250.187.206"), "142.250.187.206");
        // Check that IPv4 mapped addresses are preserved
        assert_eq!(ip_key("::FFFF:142.250.187.206"), "142.250.187.206");
        // Check that IPv6 addresses are grouped into /64 subnets
        assert_eq!(
            ip_key("2a00:1450:4009:81f::200e"),
            "2a00:1450:4009:81f::/64"
        );
    }
}
