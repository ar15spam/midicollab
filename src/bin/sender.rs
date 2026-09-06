use midir::MidiOutput;
use std::{thread, time::Duration};

fn main() {
    let midi_output = MidiOutput::new("collab_sender").expect("Couldn't create an output"); 

    let ports = midi_output.ports(); 

    if ports.is_empty() {
        println!("No output ports");
        return; 
    }

    for (i, port) in ports.iter().enumerate() {
        let name = midi_output.port_name(port).unwrap_or_else(|_| "Unknown".to_string()); 
        println!("{i}: {name}"); 
    }

    let port = &ports[0]; 

    let mut connection = midi_output.connect(port, "collab-output").expect("failed to connect to output"); 

    connection.send(&[0x90, 60, 100]).unwrap();
    connection.send(&[0x90, 64, 100]).unwrap();
    connection.send(&[0x90, 67, 100]).unwrap();

    thread::sleep(Duration::from_millis(1000));

    connection.send(&[0x80, 60, 0]).unwrap();
    connection.send(&[0x80, 64, 0]).unwrap();
    connection.send(&[0x80, 67, 0]).unwrap();
}