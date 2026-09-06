mod midi;

use midi::{NetworkEvent, parse_midi};

use midir::{Ignore, MidiInput};

use std::io::{stdin, Read, Write};
use std::net::TcpStream;
use std::thread;

fn main() {
    let mut midi_in =
        MidiInput::new("midi-collab").expect("Couldn't create MIDI input");

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

    println!("Connecting to MIDI port: {port_name}");

    let mut tcp_stream = TcpStream::connect("127.0.0.1:9000").expect("Failed to connect to TCP server");

    println!("Connected to TCP server");

    let mut read_stream = tcp_stream.try_clone().expect("Couldn't clone TCP stream");

    thread::spawn(move || {
        loop {
            let mut buffer = [0u8; 5];

            match read_stream.read_exact(&mut buffer) {
                Ok(_) => {
                    if let Some(network_event) = NetworkEvent::network_event_from_bytes(&buffer) {
                        println!(
                            "REMOTE client {}: {:?}",
                            network_event.sender_id,
                            network_event.event
                        );
                    }
                }

                Err(error) => {
                    println!("Server disconnected: {error}");
                    break;
                }
            }
        }
    });

    let _connection = midi_in
        .connect(
            port,
            "midi_collab",
            move |timestamp, message, _| {
                if let Some(event) = parse_midi(message) {
                    println!("LOCAL {timestamp}: {event:?}");

                   
                    let bytes = event.to_bytes();

                    if let Err(error) = tcp_stream.write_all(&bytes) {
                        println!("Failed to send MIDI event: {error}");
                    }
                }
            },
            (),
        )
        .expect("Failed to connect to MIDI port");

    
    let mut input = String::new();
    stdin().read_line(&mut input).unwrap();
}
