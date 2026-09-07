#[path = "../midi.rs"]
mod midi;

use midi::{ClientId, MidiState, NetworkEvent, RoomId};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};

const ROOM_EVENT_CAPACITY: usize = 1024;

#[derive(Clone)]
struct Room {
    tx: broadcast::Sender<NetworkEvent>,
    clients: usize,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:9000").await?;
    println!("Server listening on 0.0.0.0:9000");

    let rooms = Arc::new(Mutex::new(HashMap::<RoomId, Room>::new()));
    let state = Arc::new(Mutex::new(MidiState::new()));
    let mut next_client_id: ClientId = 0;

    loop {
        let (stream, address) = listener.accept().await?;

        let client_id = next_client_id;
        next_client_id = next_client_id
            .checked_add(1)
            .expect("client ID space exhausted");

        let client_rooms = Arc::clone(&rooms);
        let client_state = Arc::clone(&state);

        tokio::spawn(async move {
            let (mut reader, mut writer) = stream.into_split();

            let mut room_buffer = [0u8; 8];
            if let Err(error) = reader.read_exact(&mut room_buffer).await {
                eprintln!("Client {client_id} from {address} failed room handshake: {error}");
                return;
            }

            let room_id = RoomId::from_be_bytes(room_buffer);

            let room_tx = {
                let mut rooms = client_rooms.lock().await;
                let room = rooms.entry(room_id).or_insert_with(|| {
                    let (tx, _) = broadcast::channel::<NetworkEvent>(ROOM_EVENT_CAPACITY);
                    Room { tx, clients: 0 }
                });

                room.clients += 1;
                room.tx.clone()
            };

            let mut room_rx = room_tx.subscribe();

            println!(
                "Client {client_id} connected from {address} and joined room {room_id}"
            );

            let mut incoming = [0u8; midi::MidiEvent::BYTE_LEN];

            loop {
                tokio::select! {
                    read_result = reader.read_exact(&mut incoming) => {
                        match read_result {
                            Ok(_) => {
                                let Some(event) = midi::event_from_bytes(&incoming) else {
                                    eprintln!("Client {client_id} sent an invalid MIDI packet");
                                    continue;
                                };

                                println!("Room {room_id}, client {client_id}: {event:?}");

                                {
                                    let mut state = client_state.lock().await;
                                    state.apply(room_id, client_id, &event);
                                }

                                let network_event = NetworkEvent {
                                    sender_id: client_id,
                                    event,
                                };

                                // Only subscribers to this room receive this event.
                                // send() failing simply means nobody else is subscribed.
                                let _ = room_tx.send(network_event);
                            }
                            Err(error) => {
                                println!("Client {client_id} disconnected: {error}");
                                break;
                            }
                        }
                    }

                    broadcast_result = room_rx.recv() => {
                        match broadcast_result {
                            Ok(network_event) => {
                                // A broadcast receiver also receives messages sent by its own task.
                                if network_event.sender_id == client_id {
                                    continue;
                                }

                                let packet = network_event.to_bytes();

                                if let Err(error) = writer.write_all(&packet).await {
                                    println!("Failed to write to client {client_id}: {error}");
                                    break;
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                eprintln!(
                                    "Client {client_id} in room {room_id} lagged and skipped {skipped} events"
                                );
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                break;
                            }
                        }
                    }
                }
            }

            {
                let mut state = client_state.lock().await;
                state.remove_client(room_id, client_id);
            }

            {
                let mut rooms = client_rooms.lock().await;

                let remove_room = if let Some(room) = rooms.get_mut(&room_id) {
                    room.clients = room.clients.saturating_sub(1);
                    room.clients == 0
                } else {
                    false
                };

                if remove_room {
                    rooms.remove(&room_id);
                    println!("Removed empty room {room_id}");
                }
            }

            println!("Removed client {client_id} from room {room_id}");
        });
    }
}
