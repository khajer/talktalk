# TalkTalk Signaling Server

A high-performance WebSocket-based signaling server built with Tokio, designed for peer-to-peer communication applications such as WebRTC video/audio calls, file sharing, and real-time data transfer.

## What is a Signaling Server?

A signaling server is an intermediary that helps establish direct peer-to-peer connections between clients. It doesn't handle the actual media or data transfer between peers—that happens directly—but it's responsible for:

- **Client Discovery**: Helping clients discover each other
- **Offer/Answer Exchange**: Facilitating the WebRTC negotiation process
- **ICE Candidate Exchange**: Sharing network connection candidates between peers
- **Connection Management**: Tracking connected and disconnected clients

## Features

- ✅ Asynchronous WebSocket communication using Tokio
- ✅ Automatic client discovery and peer list broadcasting
- ✅ Support for WebRTC Offer/Answer exchange
- ✅ ICE candidate forwarding
- ✅ Real-time connection management
- ✅ Structured logging with tracing
- ✅ Thread-safe client state management
- ✅ Graceful connection handling

## Installation

### Prerequisites

- Rust 1.70 or higher
- Cargo (comes with Rust)

### Build from Source

```bash
git clone <repository-url>
cd talktalk
cargo build --release
```

## Running the Server

```bash
cargo run
```

The server will start listening on `127.0.0.1:8080` by default.

You should see output like:

```
INFO Starting signaling server at 127.0.0.1:8080
INFO Server listening on 127.0.0.1:8080
```

## Message Protocol

All messages are sent as JSON with the following structure:

### Message Types

#### 1. Join (Server → Client)
Sent to existing clients when a new client connects.

```json
{
  "type": "join",
  "client_id": "uuid-of-new-client"
}
```

#### 2. Leave (Server → Client)
Sent to clients when another client disconnects.

```json
{
  "type": "leave",
  "client_id": "uuid-of-leaving-client"
}
```

#### 3. Peers (Server → Client)
Sent to a new client upon connection, listing all existing peers.

```json
{
  "type": "peers",
  "peers": ["client-id-1", "client-id-2", "client-id-3"]
}
```

#### 4. Offer (Client → Server → Client)
Used to initiate a WebRTC connection.

```json
{
  "type": "offer",
  "from": "sender-client-id",
  "to": "target-client-id",
  "sdp": "session-description-protocol-string"
}
```

#### 5. Answer (Client → Server → Client)
Response to an offer in WebRTC negotiation.

```json
{
  "type": "answer",
  "from": "responder-client-id",
  "to": "offerer-client-id",
  "sdp": "session-description-protocol-string"
}
```

#### 6. ICE Candidate (Client → Server → Client)
Network connection candidates used for establishing the direct connection.

```json
{
  "type": "ice-candidate",
  "from": "sender-client-id",
  "to": "target-client-id",
  "candidate": "candidate-string",
  "sdp_mid": "optional-sdp-mid",
  "sdp_mline_index": 0
}
```

## Example Usage Flow

### Step 1: Client A Connects

1. Client A connects to `ws://127.0.0.1:8080`
2. Server assigns Client A a unique ID
3. Server sends Client A the current peer list (empty)

### Step 2: Client B Connects

1. Client B connects to `ws://127.0.0.1:8080`
2. Server assigns Client B a unique ID
3. Server sends Client B the current peer list (containing Client A)
4. Server sends Client A a "join" message with Client B's ID

### Step 3: WebRTC Negotiation

1. Client A creates an offer and sends it to the server, addressed to Client B
2. Server forwards the offer to Client B
3. Client B creates an answer and sends it to the server, addressed to Client A
4. Server forwards the answer to Client A
5. Both clients exchange ICE candidates through the server
6. Direct peer-to-peer connection is established

## Client Example (JavaScript/WebSocket)

```javascript
const ws = new WebSocket('ws://127.0.0.1:8080');

ws.onopen = () => {
  console.log('Connected to signaling server');
};

ws.onmessage = (event) => {
  const message = JSON.parse(event.data);
  
  switch (message.type) {
    case 'peers':
      console.log('Available peers:', message.peers);
      break;
    case 'join':
      console.log('New peer joined:', message.client_id);
      break;
    case 'leave':
      console.log('Peer left:', message.client_id);
      break;
    case 'offer':
      handleOffer(message.from, message.sdp);
      break;
    case 'answer':
      handleAnswer(message.from, message.sdp);
      break;
    case 'ice-candidate':
      handleIceCandidate(message.from, message.candidate);
      break;
  }
};

function sendOffer(targetId, sdp) {
  ws.send(JSON.stringify({
    type: 'offer',
    to: targetId,
    from: myClientId,
    sdp: sdp
  }));
}
```

## Architecture

```
┌─────────────┐
│   Client A  │
└──────┬──────┘
       │ WebSocket
       │
┌──────▼──────┐
│  Signaling  │
│   Server    │
└──────┬──────┘
       │ WebSocket
       │
┌──────▼──────┐
│   Client B  │
└─────────────┘

[Signaling Server handles metadata only]
[Actual P2P data flows directly between clients]
```

## Development

### Dependencies

- `tokio`: Async runtime
- `tokio-tungstenite`: WebSocket implementation
- `futures-util`: Async utilities
- `serde` & `serde_json`: Serialization/deserialization
- `uuid`: Unique ID generation
- `tracing` & `tracing-subscriber`: Structured logging

### Running Tests

```bash
cargo test
```

### Building with Optimizations

```bash
cargo build --release
```

The optimized binary will be located at `target/release/talktalk`.

## License

This project is licensed under the terms of the LICENSE file.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests.
