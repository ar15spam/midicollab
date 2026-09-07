use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, Mutex};

type RoomId = u64;
type ClientId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MidiEvent {
    NoteOn {
        channel: u8,
        note: u8,
        velocity: u8,
    },
    NoteOff {
        channel: u8,
        note: u8,
        velocity: u8,
    },
}

#[derive(Debug, Clone, Serialize)]
struct RoomEvent {
    #[serde(rename = "type")]
    kind: &'static str,
    sender_id: ClientId,
    event: MidiEvent,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMessage {
    Join { room_id: RoomId },
    Midi { event: MidiEvent },
}

#[derive(Clone)]
struct Room {
    tx: broadcast::Sender<RoomEvent>,
}

#[derive(Clone)]
struct AppState {
    rooms: Arc<Mutex<HashMap<RoomId, Room>>>,
    next_client_id: Arc<AtomicU64>,
}

#[tokio::main]
async fn main() {
    let state = AppState {
        rooms: Arc::new(Mutex::new(HashMap::new())),
        next_client_id: Arc::new(AtomicU64::new(1)),
    };

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/ws", get(ws_handler))
        .with_state(state);

    let address = SocketAddr::from(([0, 0, 0, 0], 8080));
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("failed to bind websocket server");

    println!("WebSocket server listening on ws://0.0.0.0:8080/ws");

    axum::serve(listener, app)
        .await
        .expect("websocket server failed");
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    let client_id = state.next_client_id.fetch_add(1, Ordering::Relaxed);

    // First browser message must be Join.
    let room_id = match socket.recv().await {
        Some(Ok(Message::Text(text))) => match serde_json::from_str::<ClientMessage>(&text) {
            Ok(ClientMessage::Join { room_id }) => room_id,
            _ => {
                let _ = socket
                    .send(Message::Text(
                        r#"{"type":"error","message":"first message must be join"}"#.into(),
                    ))
                    .await;
                return;
            }
        },
        _ => return,
    };

    let room_tx = {
        let mut rooms = state.rooms.lock().await;
        rooms
            .entry(room_id)
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(1024);
                Room { tx }
            })
            .tx
            .clone()
    };

    let mut room_rx = room_tx.subscribe();

    let joined = serde_json::json!({
        "type": "joined",
        "room_id": room_id,
        "client_id": client_id
    });

    if socket.send(Message::Text(joined.to_string().into())).await.is_err() {
        return;
    }

    println!("Web client {client_id} joined room {room_id}");

    let (mut ws_sender, mut ws_receiver) = socket.split();

    loop {
        tokio::select! {
            incoming = ws_receiver.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(ClientMessage::Midi { event }) =
                            serde_json::from_str::<ClientMessage>(&text)
                        {
                            let _ = room_tx.send(RoomEvent {
                                kind: "midi",
                                sender_id: client_id,
                                event,
                            });
                        }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    _ => {}
                }
            }

            room_event = room_rx.recv() => {
                match room_event {
                    Ok(event) => {
                        if event.sender_id == client_id {
                            continue;
                        }

                        let payload = match serde_json::to_string(&event) {
                            Ok(payload) => payload,
                            Err(_) => continue,
                        };

                        if ws_sender.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    println!("Web client {client_id} left room {room_id}");
}
