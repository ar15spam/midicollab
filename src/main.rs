mod midi;

use midi::parse_midi;
use midir::{Ignore, MidiInput}; 
use std::io::stdin; 

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

    let _connection = midi_in.connect(
        port, 
        "midi_collab", 
        |timestamp, message, _| {
            if let Some(event) = parse_midi(message) {
                println!("{timestamp}: {event:?}");
            }
        },
        (), 
    ).expect("failed to connect to the port"); 

    let mut input = String::new();
    stdin().read_line(&mut input).unwrap();
}

