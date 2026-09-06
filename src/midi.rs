

#[derive(Debug)]
pub enum MidiEvent {
    NoteOn {
        channel: u8, 
        note: u8, 
        velocity: u8
    }, 
    NoteOff {
        channel: u8, 
        note: u8,
        velocity: u8
    }
}

pub fn parse_midi(message: &[u8]) -> Option<MidiEvent> {

    if message.len() < 3 {
        return None; 
    }

    let status = message[0]; 
    let message_type = status & 0xF0; 
    let channel = status & 0x0F; 
    let note = message[1]; 
    let velocity = message[2]; 

    match message_type {
        0x90 if velocity == 0 => Some(MidiEvent::NoteOff { channel, note, velocity }), 
        0x90 => Some(MidiEvent::NoteOn { channel, note, velocity }),
        0x80 => Some(MidiEvent::NoteOff { channel, note, velocity }),
        _ => None, 
    }
}