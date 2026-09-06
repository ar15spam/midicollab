#[path = "../midi.rs"]
mod midi;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

struct Client {
    id: usize,
    stream: TcpStream,
}

fn main() {
    let listener =
        TcpListener::bind("127.0.0.1:9000").expect("Failed to bind TCP listener");

    println!("Server listening on 127.0.0.1:9000");

    let state = Arc::new(Mutex::new(midi::MidiState::new()));
    let clients = Arc::new(Mutex::new(Vec::<Client>::new()));

    let mut next_client_id = 0usize;

    for stream in listener.incoming() {
        let mut stream = stream.expect("Failed to accept connection");

        let client_id = next_client_id;
        next_client_id += 1;

        let write_stream = stream
            .try_clone()
            .expect("Failed to clone TCP stream");

        clients.lock().unwrap().push(Client {
            id: client_id,
            stream: write_stream,
        });

        println!("Client {client_id} connected");

        let client_state = Arc::clone(&state);
        let client_list = Arc::clone(&clients);

        thread::spawn(move || {
            loop {
                // Clients send plain MidiEvent packets: 4 bytes.
                let mut buffer = [0u8; 4];

                match stream.read_exact(&mut buffer) {
                    Ok(_) => {
                        if let Some(event) = midi::event_from_bytes(&buffer) {
                            println!("Client {client_id}: {event:?}");

                            {
                                let mut state = client_state.lock().unwrap();
                                state.apply(client_id, &event);

                                println!("Server state: {:?}", state.active_notes());
                            }

                            // The server adds trusted sender identity before broadcasting.
                            let network_event = midi::NetworkEvent {
                                sender_id: client_id as u8,
                                event,
                            };

                            let packet = network_event.to_bytes();

                            let mut clients = client_list.lock().unwrap();

                            clients.retain_mut(|client| {
                                // Do not echo the event back to the sender.
                                if client.id == client_id {
                                    return true;
                                }

                                match client.stream.write_all(&packet) {
                                    Ok(_) => true,
                                    Err(error) => {
                                        println!(
                                            "Removing client {}: {}",
                                            client.id, error
                                        );
                                        false
                                    }
                                }
                            });
                        }
                    }

                    Err(error) => {
                        println!("Client {client_id} disconnected: {error}");
                        break;
                    }
                }
            }

            {
                let mut state = client_state.lock().unwrap();
                state.remove_client(client_id);
            }

            client_list
                .lock()
                .unwrap()
                .retain(|client| client.id != client_id);

            println!("Removed client {client_id}");
        });
    }
}
