use tracing_subscriber::fmt;

pub fn setup_logging() {
    fmt()
        .with_target(false)
        .with_max_level(tracing::Level::INFO)
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setup_logging_does_not_panic() {
        setup_logging();
        tracing::info!("test log line");
    }
}
