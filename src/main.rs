mod logging;
mod signaling;

use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{info, error};

use logging::setup_logging;
use signaling::{handle_connection, SignalingServer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_logging();

    let host = "127.0.0.1:8080";
    info!("Starting signaling server at {}", host);

    let server = Arc::new(SignalingServer::new());
    let listener = TcpListener::bind(host).await?;

    info!("Server listening on {}", host);

    while let Ok((stream, addr)) = listener.accept().await {
        let server = server.clone();

        tokio::spawn(async move {
            match tokio_tungstenite::accept_async(stream).await {
                Ok(websocket) => {
                    info!("New WebSocket connection from {}", addr);
                    handle_connection(addr, websocket, server).await;
                }
                Err(e) => {
                    error!("Error during WebSocket handshake: {:?}", e);
                }
            }
        });
    }

    Ok(())
}
