#[path="../midi.rs"]
mod midi; 

use std::io::{Read, Write};
use std::net::TcpStream;

fn main() {
    let mut stream = TcpStream::connect("127.0.0.1:9000").expect("Failed to connect to server");

    println!("Connected to server");

    let event = midi::MidiEvent::NoteOn { channel: 0, note: 60, velocity: 100 }; 

    let bytes = event.to_bytes(); 

    stream.write_all(&bytes).expect("couldnt send the bytes"); 
}