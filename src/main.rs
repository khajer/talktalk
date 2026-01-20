use tracing::info;
use tracing_subscriber::fmt;
use std::io;
use tokio::net::UdpSocket;

fn setup_logging() {
    fmt()
        .with_target(false)
        .with_max_level(tracing::Level::INFO)
        .init();
}

#[tokio::main]
async fn main() -> io::Result<()> {
    setup_logging();
    let host = "127.0.0.1:6772";

    info!("Starting server at {}", host);
    let sock = UdpSocket::bind(host).await?;

    let mut buf = [0; 1024];
    loop {

        let (len, addr) = sock.recv_from(&mut buf).await?;
        println!("Received {} bytes from {}", len, addr);

        sock.send_to(&buf[..len], addr).await?;

    }

}
