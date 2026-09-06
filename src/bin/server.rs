#[path = "../midi.rs"]
mod midi;

use midi::{MidiState, NetworkEvent};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9000").await?;
    println!("Server listening on 127.0.0.1:9000");

    let state = Arc::new(Mutex::new(MidiState::new()));
    let mut next_client_id = 0usize;


    let (broadcast_tx, _) = broadcast::channel::<NetworkEvent>(1024);

    loop {
        let (stream, address) = listener.accept().await?;

        let client_id = next_client_id;
        next_client_id += 1;

        if client_id > u8::MAX as usize {
            eprintln!("Rejecting {address}: protocol supports at most 256 client IDs");
            continue;
        }

        println!("Client {client_id} connected from {address}");

        let client_state = Arc::clone(&state);
        let client_broadcast_tx = broadcast_tx.clone();
        let mut broadcast_rx = broadcast_tx.subscribe();

        tokio::spawn(async move {
            let (mut reader, mut writer) = stream.into_split();
            let mut incoming = [0u8; 4];

            loop {
                tokio::select! {
                    read_result = reader.read_exact(&mut incoming) => {
                        match read_result {
                            Ok(_) => {
                                let Some(event) = midi::event_from_bytes(&incoming) else {
                                    eprintln!("Client {client_id} sent an invalid MIDI packet");
                                    continue;
                                };

                                println!("Client {client_id}: {event:?}");

                                {
                                    let mut state = client_state.lock().await;
                                    state.apply(client_id, &event);
                                    println!("Server state: {:?}", state.active_notes());
                                }

                                let network_event = NetworkEvent {
                                    sender_id: client_id as u8,
                                    event,
                                };

                                if let Err(error) = client_broadcast_tx.send(network_event) {
                                    eprintln!("Broadcast failed: {error}");
                                }
                            }
                            Err(error) => {
                                println!("Client {client_id} disconnected: {error}");
                                break;
                            }
                        }
                    }

                    broadcast_result = broadcast_rx.recv() => {
                        match broadcast_result {
                            Ok(network_event) => {
                                if network_event.sender_id as usize == client_id {
                                    continue;
                                }

                                let packet = network_event.to_bytes();

                                if let Err(error) = writer.write_all(&packet).await {
                                    println!("Failed to write to client {client_id}: {error}");
                                    break;
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                eprintln!("Client {client_id} lagged and skipped {skipped} events");
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
                state.remove_client(client_id);
            }

            println!("Removed client {client_id}");
        });
    }
}
