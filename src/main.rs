use tracing::info;
use tracing_subscriber::fmt;

fn setup_logging() {
    fmt()
        .with_target(false)
        .with_max_level(tracing::Level::INFO)
        .init();
}

#[tokio::main]
async fn main(){
    setup_logging();
    let host = "127.0.0.1:6772";

    info!("Starting server at {}", host);
}
