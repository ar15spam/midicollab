mod midi;

use midi::{parse_midi, NetworkEvent, RoomId};
use midir::{Ignore, MidiInput, MidiOutput};
use std::env;
use std::sync::mpsc as std_mpsc;
use std::thread;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Usage:
    //   cargo run -- <room_id> <input_port_index> <output_port_index>
    // Example:
    //   cargo run -- 42 0 1
    //
    // On macOS, use DIFFERENT IAC buses for input and remote output.
    // Listening to and writing back into the same IAC bus can create a MIDI
    // feedback loop where remote notes are re-sent to the server indefinitely.
    let room_id: RoomId = env::args()
        .nth(1)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(42);

    let input_port_index: usize = env::args()
        .nth(2)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(0);

    let output_port_index: usize = env::args()
        .nth(3)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(0);

    let mut midi_in = MidiInput::new("midi-collab-input")?;
    midi_in.ignore(Ignore::None);

    let midi_out = MidiOutput::new("midi-collab-output")?;

    let input_ports = midi_in.ports();
    let output_ports = midi_out.ports();

    if input_ports.is_empty() {
        println!("No MIDI input ports");
        return Ok(());
    }

    if output_ports.is_empty() {
        println!("No MIDI output ports");
        return Ok(());
    }

    println!("MIDI input ports:");
    for (i, port) in input_ports.iter().enumerate() {
        let name = midi_in
            .port_name(port)
            .unwrap_or_else(|_| "Unknown".to_string());
        println!("  {i}: {name}");
    }

    println!("MIDI output ports:");
    for (i, port) in output_ports.iter().enumerate() {
        let name = midi_out
            .port_name(port)
            .unwrap_or_else(|_| "Unknown".to_string());
        println!("  {i}: {name}");
    }

    let Some(input_port) = input_ports.get(input_port_index) else {
        eprintln!("Invalid MIDI input port index {input_port_index}");
        return Ok(());
    };

    let Some(output_port) = output_ports.get(output_port_index) else {
        eprintln!("Invalid MIDI output port index {output_port_index}");
        return Ok(());
    };

    let input_name = midi_in
        .port_name(input_port)
        .unwrap_or_else(|_| "Unknown".to_string());
    let output_name = midi_out
        .port_name(output_port)
        .unwrap_or_else(|_| "Unknown".to_string());

    println!("Local MIDI input:  {input_name}");
    println!("Remote MIDI output: {output_name}");

    if input_name == output_name {
        eprintln!(
            "WARNING: input and output have the same port name. On an IAC bus this may cause a feedback loop."
        );
    }

    let mut output_connection = midi_out.connect(output_port, "midi-collab-remote-output")?;
    let (output_tx, output_rx) = std_mpsc::channel::<[u8; 3]>();

    let output_thread = thread::spawn(move || {
        while let Ok(message) = output_rx.recv() {
            if let Err(error) = output_connection.send(&message) {
                eprintln!("Failed to emit remote MIDI: {error}");
                break;
            }
        }
    });

    let server_address = env::args()
        .nth(4)
        .unwrap_or_else(|| "127.0.0.1:9000".to_string());

    println!("Connecting to server: {server_address}");
    let mut stream = TcpStream::connect(&server_address).await?;

    stream.write_all(&room_id.to_be_bytes()).await?;
    println!("Connected to {server_address} and joined room {room_id}");

    let (mut network_reader, mut network_writer) = stream.into_split();

    let (midi_tx, mut midi_rx) = mpsc::unbounded_channel::<[u8; midi::MidiEvent::BYTE_LEN]>();

    let writer_task = tokio::spawn(async move {
        while let Some(packet) = midi_rx.recv().await {
            if let Err(error) = network_writer.write_all(&packet).await {
                eprintln!("Failed to send MIDI event: {error}");
                break;
            }
        }
    });

    let reader_task = tokio::spawn(async move {
        let mut buffer = [0u8; NetworkEvent::BYTE_LEN];

        loop {
            match network_reader.read_exact(&mut buffer).await {
                Ok(_) => {
                    if let Some(network_event) = NetworkEvent::from_bytes(&buffer) {
                        println!(
                            "REMOTE client {}: {:?}",
                            network_event.sender_id,
                            network_event.event
                        );

                        let raw_midi = network_event.event.to_midi_bytes();

                        if output_tx.send(raw_midi).is_err() {
                            eprintln!("MIDI output thread is no longer running");
                            break;
                        }
                    }
                }
                Err(error) => {
                    println!("Server disconnected: {error}");
                    break;
                }
            }
        }
    });

    let _midi_connection = midi_in.connect(
        input_port,
        "midi-collab-local-input",
        move |timestamp, message, _| {
            if let Some(event) = parse_midi(message) {
                println!("LOCAL {timestamp}: {event:?}");

                if midi_tx.send(event.to_bytes()).is_err() {
                    eprintln!("Network writer task is no longer running");
                }
            }
        },
        (),
    )?;

    println!(
        "MIDI client running in room {room_id}. Local input -> server -> remote clients -> {output_name}"
    );
    println!("Press Ctrl-C to stop.");

    tokio::signal::ctrl_c().await?;

    writer_task.abort();
    reader_task.abort();

    let _ = writer_task.await;
    let _ = reader_task.await;

    let _ = output_thread.join();

    Ok(())
}
