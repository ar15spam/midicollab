mod midi;

use std::sync::{Arc, Mutex};
use midi::{parse_midi, MidiState};
use midir::{Ignore, MidiInput}; 
use std::io::stdin; 
use std::io::Write;
use std::net::TcpStream;

fn main() {

    let mut midi_in = MidiInput::new("midi-collab").expect("Couldn't create an input");

    midi_in.ignore(Ignore::None);

    let ports = midi_in.ports(); 

    if ports.is_empty() {
        println!("No input ports"); 
        return; 
    }

    for (i, port) in ports.iter().enumerate() {
        let name = midi_in
            .port_name(port)
            .unwrap_or_else(|_| "Unknown".to_string());

        println!("{i}: {name}");
    }

    let port = &ports[0]; 
    let port_name = midi_in.port_name(port).unwrap_or_else(|_| "Unknown".to_string()); 

    println!("Connecting to {}", port_name); 

    let state = Arc::new(Mutex::new(MidiState::new()));
    let callback_state = Arc::clone(&state);

    let mut tcp_stream = TcpStream::connect("127.0.0.1:9000").expect("Failed to connect to TCP server");
    let read_stream = tcp_stream.try_clone().expect("couldn't clone the read stream"); 

    let _connection = midi_in.connect(
        port, 
        "midi_collab", 
        move |timestamp, message, _| {
            if let Some(event) = parse_midi(message) {
                println!("{timestamp}: {event:?}");

                {
                    let mut state = callback_state.lock().unwrap();
                    state.apply(&event);
                    println!("Active notes: {:?}", state.active_notes());
                }

                let bytes = event.to_bytes();
                tcp_stream.write_all(&bytes).expect("failed"); 
            }
        },
        (), 
    ).expect("failed to connect to the port"); 

    let mut input = String::new();
    stdin().read_line(&mut input).unwrap();
}

