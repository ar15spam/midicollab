use std::collections::{HashSet, HashMap}; 

#[derive(Debug)]
pub struct NetworkEvent {
    pub sender_id: u8,
    pub event: MidiEvent,
}

impl NetworkEvent {
    pub fn to_bytes(&self) -> [u8; 5] {
        let event_bytes = self.event.to_bytes();

        [
            self.sender_id,
            event_bytes[0],
            event_bytes[1],
            event_bytes[2],
            event_bytes[3],
        ]
    }

    pub fn network_event_from_bytes(bytes: &[u8]) -> Option<NetworkEvent> {
        if bytes.len() != 5 {
            return None;
        }

        let event = event_from_bytes(&bytes[1..])?;

        Some(NetworkEvent {
            sender_id: bytes[0],
            event,
        })
    }
}

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
    active_notes: HashMap<usize, HashSet<(u8, u8)>> //channel, note 
}

impl MidiState {
    pub fn new() -> Self {
        Self {
            active_notes: HashMap::new(), 
        }
    }

    pub fn active_notes(&self) -> &HashMap<usize, HashSet<(u8, u8)>> {
        &self.active_notes
    }

    pub fn apply(&mut self, client_id: usize,  event: &MidiEvent) {
        match event {
            MidiEvent::NoteOn { channel, note, .. } => { self.active_notes.entry(client_id).or_insert_with(HashSet::new).insert((*channel, *note)); },
            MidiEvent::NoteOff { channel, note, .. } => { 
                if let Some(notes) = self.active_notes.get_mut(&client_id) {
                    notes.remove(&(*channel, *note)); 
                }
            }, 
            MidiEvent::ControlChange { .. } => {}, 
        }
    }

    pub fn remove_client(&mut self, client_id: usize) {
        self.active_notes.remove(&client_id);
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