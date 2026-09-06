use std::collections::HashSet; 

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
    },
    ControlChange {
        channel: u8,
        controller: u8,
        value: u8,
    },
}

impl MidiEvent {
    pub fn to_bytes(&self) -> [u8; 4] {
        match self {
            MidiEvent::NoteOn { channel, note, velocity } => [1, *channel, *note, *velocity], 
            MidiEvent::NoteOff { channel, note, velocity } => [2, *channel, *note, *velocity], 
            MidiEvent::ControlChange { channel, controller, value } => [3, *channel, *controller, *value], 
        }
    }
}

pub fn event_from_bytes(bytes: &[u8]) -> Option<MidiEvent> {
    if bytes.len() != 4 {
        return None;
    }

    match bytes[0] {
        1 => Some(MidiEvent::NoteOn { channel: bytes[1], note: bytes[2], velocity: bytes[3] }),
        2 => Some(MidiEvent::NoteOff { channel: bytes[1], note: bytes[2], velocity: bytes[3] }), 
        3 => Some(MidiEvent::ControlChange { channel: bytes[1], controller: bytes[2], value: bytes[3] }),
        _ => None,
    }
}

#[derive(Debug)]
pub struct MidiState {
    active_notes: HashSet<(u8, u8)> //channel, note 
}

impl MidiState {
    pub fn new() -> Self {
        Self {
            active_notes: HashSet::new(), 
        }
    }

    pub fn active_notes(&self) -> &HashSet<(u8, u8)> {
        &self.active_notes
    }

    pub fn apply(&mut self, event: &MidiEvent) {
        match event {
            MidiEvent::NoteOn { channel, note, .. } => { self.active_notes.insert((*channel, *note)); },
            MidiEvent::NoteOff { channel, note, .. } => { self.active_notes.remove(&(*channel, *note)); }, 
            MidiEvent::ControlChange { .. } => {}, 
        }
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
        0xB0 => Some(MidiEvent::ControlChange { channel, controller: note, value: velocity }),
        _ => None, 
    }
}