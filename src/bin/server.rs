#[path="../midi.rs"]
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
    let listener = TcpListener::bind("127.0.0.1:9000").expect("Failed to bind TCP listener");

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

        println!("Client connected");

        let client_state = Arc::clone(&state);
        let client_list = Arc::clone(&clients);

        thread::spawn(move || {
            loop {
                let mut buffer = [0u8; 4];

                match stream.read_exact(&mut buffer) {
                    Ok(_) => {
                        if let Some(event) = midi::event_from_bytes(&buffer) {
                            println!("Received event: {event:?}");

                            {
                                let mut state = client_state.lock().unwrap();

                                state.apply(&event);

                                println!(
                                    "Server state: {:?}",
                                    state.active_notes()
                                );
                            }
                        }

                        let mut clients = client_list.lock().unwrap();

                        clients.retain_mut(|client| {

                            if client.id == client_id {
                                return true; 
                            }

                            match client.stream.write_all(&buffer) {
                                Ok(_) => true,

                                Err(error) => {
                                    println!(
                                        "Removing disconnected client: {error}"
                                    );

                                    false
                                }
                            }
                        });
                    }

                    Err(error) => {
                        println!("Client disconnected: {error}");
                        break;
                    }
                }
            }

            client_list.lock().unwrap().retain(|client| client.id != client_id);
    });
}
}