# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

- Build: `cargo build`
- Build (release): `cargo build --release`
- Run: `cargo run` (starts server on `127.0.0.1:8080`)
- Test: `cargo test`
- Lint: `cargo clippy`
- Format: `cargo fmt`

There is currently no test suite and only a single source file (`src/main.rs`), so there is no per-test filtering to document yet.

## Architecture

TalkTalk is a WebRTC signaling server: a single-binary Tokio async application, all logic in `src/main.rs`. It relays JSON control messages between WebSocket clients but never touches media/data streams itself — those flow peer-to-peer once WebRTC negotiation completes.

Key pieces:

- `SignalMessage` — a serde-tagged enum (`#[serde(tag = "type")]`) that is both the wire protocol and the internal message type. Variants: `offer`, `answer`, `ice-candidate` (all client→server→client, routed by `to`/`from` fields), and `join`/`leave`/`peers` (server→client only, used for presence).
- `SignalingServer` — holds the single shared piece of state: `clients: Arc<RwLock<HashMap<String, Client>>>`, keyed by a server-generated UUID per connection. All connection handlers share one `Arc<SignalingServer>`.
- `Client` — wraps an `mpsc::UnboundedSender<Message>` used to push outbound frames to that client's writer task.
- `handle_connection` — per-connection entry point. Splits the WebSocket into read/write halves, spawns two concurrent loops via `tokio::select!`:
  - a receive loop that parses incoming JSON into `SignalMessage` and dispatches via `handle_message` (routes `offer`/`answer`/`ice-candidate` to the target client's channel; ignores `join`/`leave`/`peers` since the server originates those itself)
  - a send loop that drains the client's mpsc channel to the WebSocket writer
  - since `tokio::select!` returns when either task finishes, disconnects are detected by either the read loop erroring/ending or the write loop failing to send.
- On connect: a new UUID is minted, the client is registered (broadcasting `join` to all other clients), then the new client is sent a `peers` message listing all existing client IDs. On disconnect, the client is removed and a `leave` is broadcast.

Because signaling is stateless-per-message and clients are addressed purely by UUID string, there's no session/auth layer — anyone who can reach the socket can address any client ID they discover through the peer list.

The full message protocol (JSON shapes for each message type) and an example client-side connection flow are documented in `README.md`.
