use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio_tungstenite::tungstenite::protocol::Message;
use futures_util::stream::StreamExt;
use futures_util::SinkExt;
use tracing::{info, error, warn};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SignalMessage {
    #[serde(rename = "offer")]
    Offer {
        from: String,
        to: String,
        sdp: String,
    },
    #[serde(rename = "answer")]
    Answer {
        from: String,
        to: String,
        sdp: String,
    },
    #[serde(rename = "ice-candidate")]
    IceCandidate {
        from: String,
        to: String,
        candidate: String,
        sdp_mid: Option<String>,
        sdp_mline_index: Option<u16>,
    },
    #[serde(rename = "join")]
    Join {
        client_id: String,
    },
    #[serde(rename = "leave")]
    Leave {
        client_id: String,
    },
    #[serde(rename = "peers")]
    Peers {
        peers: Vec<String>,
    },
}

#[derive(Clone)]
struct Client {
    id: String,
    sender: mpsc::UnboundedSender<Message>,
}

impl Client {
    fn new(_addr: SocketAddr, sender: mpsc::UnboundedSender<Message>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            sender,
        }
    }
}

pub struct SignalingServer {
    clients: Arc<RwLock<HashMap<String, Client>>>,
}

impl SignalingServer {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn add_client(&self, client: Client) {
        let mut clients = self.clients.write().await;
        let client_id = client.id.clone();
        clients.insert(client_id.clone(), client);


        info!("Client connected: {client_id} (total: {})",  clients.len());
        // Notify other clients about the new peer
        self.broadcast_to_others(
            &client_id,
            SignalMessage::Join {
                client_id: client_id.clone(),
            }
        ).await;
    }

    async fn remove_client(&self, client_id: &str) {
        let mut clients = self.clients.write().await;
        if clients.remove(client_id).is_some() {
            info!("Client disconnected: {client_id} (total: {})" , clients.len());

            // Notify other clients about the peer leaving
            self.broadcast_to_others(
                client_id,
                SignalMessage::Leave {
                    client_id: client_id.to_string(),
                }
            ).await;
        }
    }



    async fn get_all_client_ids(&self) -> Vec<String> {
        let clients = self.clients.read().await;
        clients.keys().cloned().collect()
    }

    async fn broadcast_to_others(&self, from_client_id: &str, message: SignalMessage) {
        let clients = self.clients.read().await;
        let message_json = match serde_json::to_string(&message) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize message: {:?}", e);
                return;
            }
        };

        for (id, client) in clients.iter() {
            if id != from_client_id {
                if let Err(e) = client.sender.send(Message::Text(message_json.clone().into())) {
                    error!("Failed to send message to {}: {:?}", id, e);
                }
            }
        }
    }

    async fn send_to_client(&self, client_id: &str, message: SignalMessage) -> bool {
        let clients = self.clients.read().await;
        if let Some(client) = clients.get(client_id) {
            let message_json = match serde_json::to_string(&message) {
                Ok(json) => json,
                Err(e) => {
                    error!("Failed to serialize message: {:?}", e);
                    return false;
                }
            };

            match client.sender.send(Message::Text(message_json.into())) {
                Ok(_) => true,
                Err(e) => {
                    error!("Failed to send message to {}: {:?}", client_id, e);
                    false
                }
            }
        } else {
            false
        }
    }

    async fn handle_message(&self, from_client_id: &str, message: SignalMessage) {
        match message {
            SignalMessage::Offer { ref to, .. } |
            SignalMessage::Answer { ref to, .. } |
            SignalMessage::IceCandidate { ref to, .. } => {
                // Forward to specific client
                let target_id = to.clone();
                if !self.send_to_client(&target_id, message).await {
                    warn!("Failed to send message to client {} (not found or disconnected)", target_id);
                }
            }
            SignalMessage::Join { .. } | SignalMessage::Leave { .. } => {
                // These are handled by add/remove client
            }
            SignalMessage::Peers { .. } => {
                // This shouldn't come from clients
                warn!("Received Peers message from client {}", from_client_id);
            }
        }
    }
}

pub async fn handle_connection(
    addr: SocketAddr,
    websocket: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    server: Arc<SignalingServer>,
) {
    let (mut write, mut read) = websocket.split();
    let (sender, mut receiver) = mpsc::unbounded_channel();

    // Create and add client
    let client = Client::new(addr, sender);
    let client_id = client.id.clone();
    server.add_client(client).await;

    // Send list of existing peers to the new client
    let existing_peers = server.get_all_client_ids().await;
    let peers_message = SignalMessage::Peers {
        peers: existing_peers,
    };
    let _ = server.send_to_client(&client_id, peers_message).await;

    // Handle incoming messages from the WebSocket
    let server_clone = server.clone();
    let client_id_clone = client_id.clone();

    let receive_task = async move {
        while let Some(result) = read.next().await {
            match result {
                Ok(message) => {
                    if let Message::Text(text) = message {
                        match serde_json::from_str::<SignalMessage>(&text) {
                            Ok(signal_msg) => {
                                server_clone.handle_message(&client_id_clone, signal_msg).await;
                            }
                            Err(e) => {
                                error!("Failed to parse signal message: {:?}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("WebSocket error: {:?}", e);
                    break;
                }
            }
        }

        server_clone.remove_client(&client_id_clone).await;
    };

    // Handle outgoing messages to the WebSocket
    let send_task = async move {
        while let Some(message) = receiver.recv().await {
            if write.send(message).await.is_err() {
                break;
            }
        }
    };

    // Run both tasks concurrently
    tokio::select! {
        _ = receive_task => {}
        _ = send_task => {}
    }
}
