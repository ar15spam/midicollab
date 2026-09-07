use std::collections::{HashMap, HashSet};

pub type ClientId = u64;
pub type RoomId = u64;

#[derive(Debug, Clone, Copy)]
pub struct NetworkEvent {
    pub sender_id: ClientId,
    pub event: MidiEvent,
}

impl NetworkEvent {
    pub const BYTE_LEN: usize = 12;

    pub fn to_bytes(&self) -> [u8; Self::BYTE_LEN] {
        let mut bytes = [0u8; Self::BYTE_LEN];
        let sender_bytes = self.sender_id.to_be_bytes();
        let event_bytes = self.event.to_bytes();

        bytes[..8].copy_from_slice(&sender_bytes);
        bytes[8..].copy_from_slice(&event_bytes);

        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::BYTE_LEN {
            return None;
        }

        let sender_id = ClientId::from_be_bytes(bytes[..8].try_into().ok()?);
        let event = event_from_bytes(&bytes[8..])?;

        Some(Self { sender_id, event })
    }
}

#[derive(Debug, Clone, Copy)]
pub enum MidiEvent {
    NoteOn {
        channel: u8,
        note: u8,
        velocity: u8,
    },
    NoteOff {
        channel: u8,
        note: u8,
        velocity: u8,
    },
    ControlChange {
        channel: u8,
        controller: u8,
        value: u8,
    },
}

impl MidiEvent {
    pub const BYTE_LEN: usize = 4;

    pub fn to_bytes(&self) -> [u8; Self::BYTE_LEN] {
        match self {
            MidiEvent::NoteOn {
                channel,
                note,
                velocity,
            } => [1, *channel, *note, *velocity],
            MidiEvent::NoteOff {
                channel,
                note,
                velocity,
            } => [2, *channel, *note, *velocity],
            MidiEvent::ControlChange {
                channel,
                controller,
                value,
            } => [3, *channel, *controller, *value],
        }
    }

    // Convert the normalized event back into a real three-byte MIDI message
    // so a remote event can be emitted to a synth, DAW, or virtual MIDI port.
    pub fn to_midi_bytes(&self) -> [u8; 3] {
        match self {
            MidiEvent::NoteOn {
                channel,
                note,
                velocity,
            } => [0x90 | (channel & 0x0F), *note, *velocity],
            MidiEvent::NoteOff {
                channel,
                note,
                velocity,
            } => [0x80 | (channel & 0x0F), *note, *velocity],
            MidiEvent::ControlChange {
                channel,
                controller,
                value,
            } => [0xB0 | (channel & 0x0F), *controller, *value],
        }
    }
}

pub fn event_from_bytes(bytes: &[u8]) -> Option<MidiEvent> {
    if bytes.len() != MidiEvent::BYTE_LEN {
        return None;
    }

    match bytes[0] {
        1 => Some(MidiEvent::NoteOn {
            channel: bytes[1],
            note: bytes[2],
            velocity: bytes[3],
        }),
        2 => Some(MidiEvent::NoteOff {
            channel: bytes[1],
            note: bytes[2],
            velocity: bytes[3],
        }),
        3 => Some(MidiEvent::ControlChange {
            channel: bytes[1],
            controller: bytes[2],
            value: bytes[3],
        }),
        _ => None,
    }
}

#[derive(Debug)]
pub struct MidiState {
    active_notes: HashMap<RoomId, HashMap<ClientId, HashSet<(u8, u8)>>>,
}

impl MidiState {
    pub fn new() -> Self {
        Self {
            active_notes: HashMap::new(),
        }
    }

    pub fn active_notes(
        &self,
    ) -> &HashMap<RoomId, HashMap<ClientId, HashSet<(u8, u8)>>> {
        &self.active_notes
    }

    pub fn apply(&mut self, room_id: RoomId, client_id: ClientId, event: &MidiEvent) {
        match event {
            MidiEvent::NoteOn { channel, note, .. } => {
                self.active_notes
                    .entry(room_id)
                    .or_default()
                    .entry(client_id)
                    .or_default()
                    .insert((*channel, *note));
            }
            MidiEvent::NoteOff { channel, note, .. } => {
                if let Some(room) = self.active_notes.get_mut(&room_id) {
                    if let Some(notes) = room.get_mut(&client_id) {
                        notes.remove(&(*channel, *note));

                        if notes.is_empty() {
                            room.remove(&client_id);
                        }
                    }

                    if room.is_empty() {
                        self.active_notes.remove(&room_id);
                    }
                }
            }
            MidiEvent::ControlChange { .. } => {}
        }
    }

    pub fn remove_client(&mut self, room_id: RoomId, client_id: ClientId) {
        if let Some(room) = self.active_notes.get_mut(&room_id) {
            room.remove(&client_id);

            if room.is_empty() {
                self.active_notes.remove(&room_id);
            }
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
    let data1 = message[1];
    let data2 = message[2];

    match message_type {
        0x90 if data2 == 0 => Some(MidiEvent::NoteOff {
            channel,
            note: data1,
            velocity: data2,
        }),
        0x90 => Some(MidiEvent::NoteOn {
            channel,
            note: data1,
            velocity: data2,
        }),
        0x80 => Some(MidiEvent::NoteOff {
            channel,
            note: data1,
            velocity: data2,
        }),
        0xB0 => Some(MidiEvent::ControlChange {
            channel,
            controller: data1,
            value: data2,
        }),
        _ => None,
    }
}
