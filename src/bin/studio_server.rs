use std::{
    collections::HashMap,
    env,
    path::{Path as FsPath, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        DefaultBodyLimit, Multipart, Path as AxumPath, State,
    },
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, Mutex};
use tower_http::{
    cors::{Any, CorsLayer},
    services::ServeDir,
};

mod token {
    //! Verifies the short-lived HMAC tokens minted by the Next.js app.
    //! Format:  base64url(payloadJson) "." base64url(hmacSha256(payloadB64))
    //! The shared key comes from REALTIME_SHARED_SECRET (same value on Vercel).

    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    use hmac::{Hmac, Mac};
    use serde::Deserialize;
    use sha2::Sha256;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Debug, Clone, Deserialize)]
    pub struct Claims {
        pub sub: String,
        #[serde(default)]
        pub name: String,
        #[serde(default)]
        pub image: Option<String>,
        pub pid: String,
        #[serde(default)]
        pub role: String,
        pub exp: u64,
    }

    #[derive(Debug)]
    pub enum TokenError {
        NotConfigured,
        Malformed,
        BadSignature,
        Expired,
        WrongProject,
    }

    pub fn secret() -> Option<String> {
        std::env::var("REALTIME_SHARED_SECRET")
            .ok()
            .filter(|value| value.len() >= 16)
    }

    /// Verify signature + expiry only (does not check which project it is for).
    pub fn verify_any(raw: &str) -> Result<Claims, TokenError> {
        let key = secret().ok_or(TokenError::NotConfigured)?;

        let (payload_b64, sig_b64) = raw.split_once('.').ok_or(TokenError::Malformed)?;

        let sig = URL_SAFE_NO_PAD
            .decode(sig_b64.as_bytes())
            .map_err(|_| TokenError::Malformed)?;

        let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes())
            .map_err(|_| TokenError::NotConfigured)?;
        mac.update(payload_b64.as_bytes());
        mac.verify_slice(&sig).map_err(|_| TokenError::BadSignature)?;

        let payload = URL_SAFE_NO_PAD
            .decode(payload_b64.as_bytes())
            .map_err(|_| TokenError::Malformed)?;
        let claims: Claims =
            serde_json::from_slice(&payload).map_err(|_| TokenError::Malformed)?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if claims.exp <= now {
            return Err(TokenError::Expired);
        }

        Ok(claims)
    }

    pub fn verify(raw: &str, expected_pid: &str) -> Result<Claims, TokenError> {
        let claims = verify_any(raw)?;
        if claims.pid != expected_pid {
            return Err(TokenError::WrongProject);
        }
        Ok(claims)
    }

    pub fn can_edit(role: &str) -> bool {
        matches!(role, "owner" | "editor")
    }
}

type ClientId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TrackKind {
    Drums,
    Synth,
    Sampler,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ClipKind {
    Drum,
    Notes,
    Sample,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SynthPreset {
    DeepBass,
    Acid,
    Pad,
    Pluck,
    Lead,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum OscillatorWave {
    Sine,
    Square,
    Sawtooth,
    Triangle,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MixerState {
    volume: f32,
    pan: f32,
    muted: bool,
    solo: bool,
    delay_send: f32,
    reverb_send: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SynthConfig {
    preset: SynthPreset,
    oscillator: OscillatorWave,
    attack: f32,
    decay: f32,
    sustain: f32,
    release: f32,
    cutoff: f32,
    resonance: f32,
    detune: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NoteEvent {
    id: String,
    note: u8,
    start_step: usize,
    duration_steps: usize,
    velocity: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SampleTrigger {
    step: usize,
    velocity: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SampleAsset {
    id: String,
    name: String,
    url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Clip {
    id: String,
    name: String,
    kind: ClipKind,
    start_bar: usize,
    length_bars: usize,
    looped: bool,
    pattern_steps: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    drum_steps: Option<HashMap<String, Vec<bool>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    notes: Option<Vec<NoteEvent>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sample_triggers: Option<Vec<SampleTrigger>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Track {
    id: String,
    name: String,
    kind: TrackKind,
    mixer: MixerState,
    #[serde(skip_serializing_if = "Option::is_none")]
    synth: Option<SynthConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sample_id: Option<String>,
    clips: Vec<Clip>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProjectState {
    project_id: String,
    name: String,
    bpm: u16,
    bars: usize,
    steps_per_bar: usize,
    playing: bool,
    start_at_ms: Option<u64>,
    loop_start_bar: usize,
    loop_end_bar: usize,
    master_volume: f32,
    #[serde(default)]
    is_public: bool,
    revision: u64,
    tracks: Vec<Track>,
    samples: Vec<SampleAsset>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ProjectOperation {
    RenameProject { name: String },
    SetBpm { bpm: u16 },
    SetBars { bars: usize },
    SetPlaying {
        playing: bool,
        start_at_ms: Option<u64>,
    },
    SetLoop {
        start_bar: usize,
        end_bar: usize,
    },
    SetMasterVolume { volume: f32 },
    SetPublic { is_public: bool },
    AddTrack { track: Track },
    DeleteTrack { track_id: String },
    RenameTrack { track_id: String, name: String },
    SetTrackMixer {
        track_id: String,
        mixer: MixerState,
    },
    SetSynth {
        track_id: String,
        synth: SynthConfig,
    },
    SetSamplerAsset {
        track_id: String,
        sample_id: Option<String>,
    },
    AddClip {
        track_id: String,
        clip: Clip,
    },
    DeleteClip {
        track_id: String,
        clip_id: String,
    },
    MoveClip {
        track_id: String,
        clip_id: String,
        start_bar: usize,
    },
    ResizeClip {
        track_id: String,
        clip_id: String,
        length_bars: usize,
    },
    RenameClip {
        track_id: String,
        clip_id: String,
        name: String,
    },
    SetDrumStep {
        track_id: String,
        clip_id: String,
        voice: String,
        step: usize,
        enabled: bool,
    },
    SetNoteCell {
        track_id: String,
        clip_id: String,
        step: usize,
        note: u8,
        enabled: bool,
        velocity: u8,
        duration_steps: usize,
    },
    SetSampleStep {
        track_id: String,
        clip_id: String,
        step: usize,
        enabled: bool,
        velocity: u8,
    },
    AddSampleAsset { asset: SampleAsset },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMessage {
    Join {
        project_id: String,
        token: String,
    },
    ProjectOperation {
        operation: ProjectOperation,
    },
}

/// Authenticated identity of a connected client, derived from the signed token.
#[derive(Debug, Clone)]
struct Identity {
    user_id: String,
    name: String,
    image: Option<String>,
    role: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct UserPresence {
    client_id: ClientId,
    user_id: String,
    name: String,
    image: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMessage {
    Joined {
        client_id: ClientId,
        project: ProjectState,
        users: Vec<UserPresence>,
    },
    ProjectState {
        sender_id: ClientId,
        project: ProjectState,
    },
    Presence {
        users: Vec<UserPresence>,
    },
    Error {
        message: String,
    },
}

struct Room {
    project: ProjectState,
    tx: broadcast::Sender<ServerMessage>,
    clients: HashMap<ClientId, Identity>,
}

#[derive(Clone)]
struct Store {
    root: PathBuf,
}

impl Store {
    async fn new(root: impl Into<PathBuf>) -> std::io::Result<Self> {
        let root = root.into();
        tokio::fs::create_dir_all(root.join("projects")).await?;
        tokio::fs::create_dir_all(root.join("samples")).await?;
        Ok(Self { root })
    }

    fn project_path(&self, project_id: &str) -> PathBuf {
        self.root
            .join("projects")
            .join(format!("{}.json", safe_id(project_id)))
    }

    async fn load_project(&self, project_id: &str) -> Option<ProjectState> {
        let bytes = tokio::fs::read(self.project_path(project_id)).await.ok()?;
        let mut project: ProjectState = serde_json::from_slice(&bytes).ok()?;
        // Transport state is per-session and never restored from disk, so a
        // page reload can't suddenly start playback.
        project.playing = false;
        project.start_at_ms = None;
        Some(project)
    }

    async fn save_project(&self, project: &ProjectState) -> std::io::Result<()> {
        // Persist a stopped transport regardless of the live room state.
        let mut on_disk = project.clone();
        on_disk.playing = false;
        on_disk.start_at_ms = None;

        let bytes = serde_json::to_vec_pretty(&on_disk)
            .map_err(std::io::Error::other)?;
        let path = self.project_path(&project.project_id);
        let temp = path.with_extension("json.tmp");
        tokio::fs::write(&temp, bytes).await?;
        tokio::fs::rename(temp, path).await
    }

    fn samples_dir(&self) -> PathBuf {
        self.root.join("samples")
    }
}

#[derive(Clone)]
struct AppState {
    rooms: Arc<Mutex<HashMap<String, Room>>>,
    next_client_id: Arc<AtomicU64>,
    next_upload_id: Arc<AtomicU64>,
    store: Store,
}

#[tokio::main]
async fn main() {
    let data_dir = env::var("DATA_DIR").unwrap_or_else(|_| "data".into());
    let store = Store::new(data_dir)
        .await
        .expect("failed to create data directories");

    let state = AppState {
        rooms: Arc::new(Mutex::new(HashMap::new())),
        next_client_id: Arc::new(AtomicU64::new(1)),
        next_upload_id: Arc::new(AtomicU64::new(1)),
        store: store.clone(),
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/ws", get(ws_handler))
        .route(
            "/api/projects/{project_id}",
            get(get_project).put(put_project),
        )
        .route("/api/samples", post(upload_sample))
        .nest_service("/samples", ServeDir::new(store.samples_dir()))
        .layer(DefaultBodyLimit::max(30 * 1024 * 1024))
        .layer(cors)
        .with_state(state);

    let port: u16 = env::var("PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8080);

    let address = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .expect("failed to bind studio server");

    println!("MIDI Collab studio server listening on {address}");
    axum::serve(listener, app).await.expect("server failed");
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    let client_id = state.next_client_id.fetch_add(1, Ordering::Relaxed);

    let (project_id, identity) = match socket.recv().await {
        Some(Ok(Message::Text(text))) => {
            match serde_json::from_str::<ClientMessage>(&text) {
                Ok(ClientMessage::Join { project_id, token })
                    if valid_project_id(&project_id) =>
                {
                    match token::verify(&token, &project_id) {
                        Ok(claims) => (
                            project_id,
                            Identity {
                                user_id: claims.sub,
                                name: {
                                    let n = clean_name(&claims.name, 40);
                                    if n.is_empty() { "Producer".to_string() } else { n }
                                },
                                image: claims.image,
                                role: claims.role,
                            },
                        ),
                        Err(reason) => {
                            eprintln!("rejected join for {project_id}: {reason:?}");
                            send_error(&mut socket, "invalid or expired invite token").await;
                            return;
                        }
                    }
                }
                _ => {
                    send_error(&mut socket, "first message must be a valid join").await;
                    return;
                }
            }
        }
        _ => return,
    };

    let stored_project = state
        .store
        .load_project(&project_id)
        .await
        .unwrap_or_else(|| default_project(&project_id));

    let (room_tx, project, users) = {
        let mut rooms = state.rooms.lock().await;

        let room = rooms.entry(project_id.clone()).or_insert_with(|| {
            let (tx, _) = broadcast::channel(256);
            Room {
                project: stored_project,
                tx,
                clients: HashMap::new(),
            }
        });

        room.clients.insert(client_id, identity.clone());

        (
            room.tx.clone(),
            room.project.clone(),
            presence(&room.clients),
        )
    };

    let mut room_rx = room_tx.subscribe();

    let joined = ServerMessage::Joined {
        client_id,
        project,
        users: users.clone(),
    };

    if send_server_message(&mut socket, &joined).await.is_err() {
        cleanup_client(&state, &project_id, client_id).await;
        return;
    }

    let _ = room_tx.send(ServerMessage::Presence { users });

    println!(
        "client {client_id} ({} / {}) joined project {project_id}",
        identity.name, identity.role
    );

    let can_edit = token::can_edit(&identity.role);
    let is_owner = identity.role == "owner";

    let (mut ws_sender, mut ws_receiver) = socket.split();

    loop {
        tokio::select! {
            incoming = ws_receiver.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(ClientMessage::ProjectOperation { operation }) =
                            serde_json::from_str::<ClientMessage>(&text)
                        {
                            if !can_edit {
                                continue;
                            }
                            // Only the owner may flip a project public/private.
                            if matches!(operation, ProjectOperation::SetPublic { .. }) && !is_owner {
                                continue;
                            }

                            let updated = {
                                let mut rooms = state.rooms.lock().await;
                                let Some(room) = rooms.get_mut(&project_id) else { break; };

                                apply_operation(&mut room.project, operation);
                                room.project.clone()
                            };

                            if let Err(error) = state.store.save_project(&updated).await {
                                eprintln!("save error for {project_id}: {error}");
                            }

                            let _ = room_tx.send(ServerMessage::ProjectState {
                                sender_id: client_id,
                                project: updated,
                            });
                        }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    _ => {}
                }
            }

            outgoing = room_rx.recv() => {
                match outgoing {
                    Ok(message) => {
                        let payload = match serde_json::to_string(&message) {
                            Ok(payload) => payload,
                            Err(_) => continue,
                        };

                        if ws_sender.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    cleanup_client(&state, &project_id, client_id).await;
    println!("client {client_id} left project {project_id}");
}

async fn cleanup_client(state: &AppState, project_id: &str, client_id: ClientId) {
    let mut rooms = state.rooms.lock().await;

    let (tx, users, remove) = if let Some(room) = rooms.get_mut(project_id) {
        room.clients.remove(&client_id);
        (
            Some(room.tx.clone()),
            presence(&room.clients),
            room.clients.is_empty(),
        )
    } else {
        (None, vec![], false)
    };

    if remove {
        rooms.remove(project_id);
    }

    drop(rooms);

    if let Some(tx) = tx {
        let _ = tx.send(ServerMessage::Presence { users });
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .map(|token| token.trim().to_string())
}

async fn get_project(
    AxumPath(project_id): AxumPath<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !valid_project_id(&project_id) {
        return (StatusCode::BAD_REQUEST, "invalid project id").into_response();
    }

    let project = {
        let rooms = state.rooms.lock().await;
        rooms.get(&project_id).map(|room| room.project.clone())
    };

    let project = match project {
        Some(project) => project,
        None => match state.store.load_project(&project_id).await {
            Some(project) => project,
            None => return (StatusCode::NOT_FOUND, "project not found").into_response(),
        },
    };

    // Public projects are readable by anyone; private ones need a valid token.
    if !project.is_public {
        let ok = bearer_token(&headers)
            .and_then(|raw| token::verify(&raw, &project_id).ok())
            .is_some();
        if !ok {
            return (StatusCode::FORBIDDEN, "project is private").into_response();
        }
    }

    Json(project).into_response()
}

async fn put_project(
    AxumPath(project_id): AxumPath<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut project): Json<ProjectState>,
) -> impl IntoResponse {
    if !valid_project_id(&project_id) {
        return (StatusCode::BAD_REQUEST, "invalid project id").into_response();
    }

    // Writing the full document requires an editor/owner token for this project.
    let authorized = bearer_token(&headers)
        .and_then(|raw| token::verify(&raw, &project_id).ok())
        .map(|claims| token::can_edit(&claims.role))
        .unwrap_or(false);
    if !authorized {
        return (StatusCode::UNAUTHORIZED, "missing or invalid token").into_response();
    }

    project.project_id = project_id.clone();
    project.revision = project.revision.saturating_add(1);

    if let Err(error) = state.store.save_project(&project).await {
        eprintln!("save error: {error}");
        return (StatusCode::INTERNAL_SERVER_ERROR, "save failed").into_response();
    }

    let tx = {
        let mut rooms = state.rooms.lock().await;
        if let Some(room) = rooms.get_mut(&project_id) {
            room.project = project.clone();
            Some(room.tx.clone())
        } else {
            None
        }
    };

    if let Some(tx) = tx {
        let _ = tx.send(ServerMessage::ProjectState {
            sender_id: 0,
            project,
        });
    }

    StatusCode::NO_CONTENT.into_response()
}

async fn upload_sample(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> impl IntoResponse {
    // Any signed, unexpired token is enough — this proves the caller is an
    // authenticated MIDICOLLAB user and keeps the upload endpoint from being
    // an open write-to-disk for the internet.
    let authorized = bearer_token(&headers)
        .and_then(|raw| token::verify_any(&raw).ok())
        .is_some();
    if !authorized {
        return (StatusCode::UNAUTHORIZED, "missing or invalid token").into_response();
    }

    while let Ok(Some(field)) = multipart.next_field().await {
        if field.name() != Some("file") {
            continue;
        }

        let original_name = field.file_name().unwrap_or("sample.wav").to_string();
        let extension = safe_extension(&original_name);
        let bytes = match field.bytes().await {
            Ok(bytes) => bytes,
            Err(_) => return (StatusCode::BAD_REQUEST, "invalid upload").into_response(),
        };

        if bytes.len() > 25 * 1024 * 1024 {
            return (StatusCode::PAYLOAD_TOO_LARGE, "sample is too large").into_response();
        }

        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();

        let serial = state.next_upload_id.fetch_add(1, Ordering::Relaxed);
        let filename = format!("{stamp}-{serial}.{extension}");
        let path = state.store.samples_dir().join(&filename);

        if tokio::fs::write(&path, bytes).await.is_err() {
            return (StatusCode::INTERNAL_SERVER_ERROR, "failed to store sample").into_response();
        }

        let asset = SampleAsset {
            id: format!("sample-{stamp}-{serial}"),
            name: clean_name(&original_name, 120),
            url: format!("/samples/{filename}"),
        };

        return Json(asset).into_response();
    }

    (StatusCode::BAD_REQUEST, "missing file field").into_response()
}

fn apply_operation(project: &mut ProjectState, operation: ProjectOperation) {
    project.revision = project.revision.saturating_add(1);

    match operation {
        ProjectOperation::RenameProject { name } => {
            project.name = clean_name(&name, 80);
        }
        ProjectOperation::SetBpm { bpm } => {
            project.bpm = bpm.clamp(50, 220);
        }
        ProjectOperation::SetBars { bars } => {
            project.bars = bars.clamp(1, 64);
            project.loop_end_bar = project.loop_end_bar.min(project.bars).max(1);
            if project.loop_start_bar >= project.loop_end_bar {
                project.loop_start_bar = 0;
                project.loop_end_bar = project.bars;
            }
        }
        ProjectOperation::SetPlaying { playing, start_at_ms } => {
            project.playing = playing;
            project.start_at_ms = if playing { start_at_ms } else { None };
        }
        ProjectOperation::SetLoop { start_bar, end_bar } => {
            project.loop_start_bar = start_bar.min(project.bars.saturating_sub(1));
            project.loop_end_bar = end_bar
                .max(project.loop_start_bar + 1)
                .min(project.bars);
        }
        ProjectOperation::SetMasterVolume { volume } => {
            project.master_volume = volume.clamp(0.0, 1.0);
        }
        ProjectOperation::SetPublic { is_public } => {
            project.is_public = is_public;
        }
        ProjectOperation::AddTrack { track } => {
            if project.tracks.len() < 64 {
                project.tracks.push(track);
            }
        }
        ProjectOperation::DeleteTrack { track_id } => {
            if project.tracks.len() > 1 {
                project.tracks.retain(|track| track.id != track_id);
            }
        }
        ProjectOperation::RenameTrack { track_id, name } => {
            if let Some(track) = find_track_mut(project, &track_id) {
                track.name = clean_name(&name, 60);
            }
        }
        ProjectOperation::SetTrackMixer { track_id, mut mixer } => {
            mixer.volume = mixer.volume.clamp(0.0, 1.0);
            mixer.pan = mixer.pan.clamp(-1.0, 1.0);
            mixer.delay_send = mixer.delay_send.clamp(0.0, 1.0);
            mixer.reverb_send = mixer.reverb_send.clamp(0.0, 1.0);
            if let Some(track) = find_track_mut(project, &track_id) {
                track.mixer = mixer;
            }
        }
        ProjectOperation::SetSynth { track_id, mut synth } => {
            synth.attack = synth.attack.clamp(0.001, 4.0);
            synth.decay = synth.decay.clamp(0.001, 4.0);
            synth.sustain = synth.sustain.clamp(0.001, 1.0);
            synth.release = synth.release.clamp(0.001, 8.0);
            synth.cutoff = synth.cutoff.clamp(40.0, 18_000.0);
            synth.resonance = synth.resonance.clamp(0.01, 30.0);
            synth.detune = synth.detune.clamp(-100.0, 100.0);

            if let Some(track) = find_track_mut(project, &track_id) {
                track.synth = Some(synth);
            }
        }
        ProjectOperation::SetSamplerAsset { track_id, sample_id } => {
            if let Some(track) = find_track_mut(project, &track_id) {
                track.sample_id = sample_id;
            }
        }
        ProjectOperation::AddClip { track_id, clip } => {
            if let Some(track) = find_track_mut(project, &track_id) {
                if track.clips.len() < 128 {
                    track.clips.push(clip);
                }
            }
        }
        ProjectOperation::DeleteClip { track_id, clip_id } => {
            if let Some(track) = find_track_mut(project, &track_id) {
                track.clips.retain(|clip| clip.id != clip_id);
            }
        }
        ProjectOperation::MoveClip {
            track_id,
            clip_id,
            start_bar,
        } => {
            let max_start = project.bars.saturating_sub(1);
            if let Some(clip) = find_clip_mut(project, &track_id, &clip_id) {
                clip.start_bar = start_bar.min(max_start);
            }
        }
        ProjectOperation::ResizeClip {
            track_id,
            clip_id,
            length_bars,
        } => {
            let max_bars = project.bars;
            if let Some(clip) = find_clip_mut(project, &track_id, &clip_id) {
                clip.length_bars = length_bars.clamp(1, max_bars);
            }
        }
        ProjectOperation::RenameClip {
            track_id,
            clip_id,
            name,
        } => {
            if let Some(clip) = find_clip_mut(project, &track_id, &clip_id) {
                clip.name = clean_name(&name, 60);
            }
        }
        ProjectOperation::SetDrumStep {
            track_id,
            clip_id,
            voice,
            step,
            enabled,
        } => {
            if let Some(clip) = find_clip_mut(project, &track_id, &clip_id) {
                if let Some(steps) = clip
                    .drum_steps
                    .as_mut()
                    .and_then(|voices| voices.get_mut(&voice))
                {
                    if step < steps.len() {
                        steps[step] = enabled;
                    }
                }
            }
        }
        ProjectOperation::SetNoteCell {
            track_id,
            clip_id,
            step,
            note,
            enabled,
            velocity,
            duration_steps,
        } => {
            if let Some(clip) = find_clip_mut(project, &track_id, &clip_id) {
                let notes = clip.notes.get_or_insert_with(Vec::new);
                notes.retain(|event| !(event.start_step == step && event.note == note));

                if enabled {
                    notes.push(NoteEvent {
                        id: format!("note-{}", now_millis()),
                        note,
                        start_step: step,
                        duration_steps: duration_steps.clamp(1, 64),
                        velocity: velocity.clamp(1, 127),
                    });
                }
            }
        }
        ProjectOperation::SetSampleStep {
            track_id,
            clip_id,
            step,
            enabled,
            velocity,
        } => {
            if let Some(clip) = find_clip_mut(project, &track_id, &clip_id) {
                let triggers = clip.sample_triggers.get_or_insert_with(Vec::new);
                triggers.retain(|trigger| trigger.step != step);

                if enabled {
                    triggers.push(SampleTrigger {
                        step,
                        velocity: velocity.clamp(1, 127),
                    });
                }
            }
        }
        ProjectOperation::AddSampleAsset { asset } => {
            if !project.samples.iter().any(|sample| sample.id == asset.id) {
                project.samples.push(asset);
            }
        }
    }
}

fn find_track_mut<'a>(
    project: &'a mut ProjectState,
    track_id: &str,
) -> Option<&'a mut Track> {
    project.tracks.iter_mut().find(|track| track.id == track_id)
}

fn find_clip_mut<'a>(
    project: &'a mut ProjectState,
    track_id: &str,
    clip_id: &str,
) -> Option<&'a mut Clip> {
    find_track_mut(project, track_id)?
        .clips
        .iter_mut()
        .find(|clip| clip.id == clip_id)
}

fn presence(clients: &HashMap<ClientId, Identity>) -> Vec<UserPresence> {
    let mut users: Vec<_> = clients
        .iter()
        .map(|(&client_id, identity)| UserPresence {
            client_id,
            user_id: identity.user_id.clone(),
            name: identity.name.clone(),
            image: identity.image.clone(),
        })
        .collect();

    users.sort_by_key(|user| user.client_id);
    users
}

async fn send_server_message(
    socket: &mut WebSocket,
    message: &ServerMessage,
) -> Result<(), axum::Error> {
    let payload = serde_json::to_string(message).unwrap_or_else(|_| {
        r#"{"type":"error","message":"serialization error"}"#.to_string()
    });
    socket.send(Message::Text(payload.into())).await
}

async fn send_error(socket: &mut WebSocket, message: &str) {
    let _ = send_server_message(
        socket,
        &ServerMessage::Error {
            message: message.to_string(),
        },
    )
    .await;
}

fn default_project(project_id: &str) -> ProjectState {
    let mut kick = vec![false; 16];
    let mut clap = vec![false; 16];
    let mut hat = vec![false; 16];
    let mut open_hat = vec![false; 16];

    for i in [0, 4, 8, 12] { kick[i] = true; }
    for i in [4, 12] { clap[i] = true; }
    for i in [2, 6, 10, 14] { hat[i] = true; }
    for i in [6, 14] { open_hat[i] = true; }

    let drums = Track {
        id: "drums".into(),
        name: "Drums".into(),
        kind: TrackKind::Drums,
        mixer: mixer(0.88, 0.0, 0.08, 0.08),
        synth: None,
        sample_id: None,
        clips: vec![Clip {
            id: "clip-drums".into(),
            name: "House drums".into(),
            kind: ClipKind::Drum,
            start_bar: 0,
            length_bars: 8,
            looped: true,
            pattern_steps: 16,
            drum_steps: Some(HashMap::from([
                ("kick".into(), kick),
                ("clap".into(), clap),
                ("hat".into(), hat),
                ("open_hat".into(), open_hat),
            ])),
            notes: None,
            sample_triggers: None,
        }],
    };

    let bass_notes = [0usize, 3, 6, 10, 12]
        .into_iter()
        .enumerate()
        .map(|(i, step)| NoteEvent {
            id: format!("bass-{i}"),
            note: if step == 10 { 39 } else { 36 },
            start_step: step,
            duration_steps: 1,
            velocity: 105,
        })
        .collect();

    let bass = Track {
        id: "bass".into(),
        name: "Deep Bass".into(),
        kind: TrackKind::Synth,
        mixer: mixer(0.72, 0.0, 0.03, 0.02),
        synth: Some(synth_preset(SynthPreset::DeepBass)),
        sample_id: None,
        clips: vec![note_clip("clip-bass", "Bassline", 0, 8, bass_notes)],
    };

    let mut chord_notes = Vec::new();
    for (i, note) in [60u8, 63, 67].into_iter().enumerate() {
        chord_notes.push(NoteEvent {
            id: format!("chord-a-{i}"),
            note,
            start_step: 0,
            duration_steps: 4,
            velocity: 78,
        });
    }
    for (i, note) in [58u8, 62, 65].into_iter().enumerate() {
        chord_notes.push(NoteEvent {
            id: format!("chord-b-{i}"),
            note,
            start_step: 8,
            duration_steps: 4,
            velocity: 76,
        });
    }

    let chords = Track {
        id: "chords".into(),
        name: "Chords".into(),
        kind: TrackKind::Synth,
        mixer: mixer(0.5, 0.0, 0.08, 0.26),
        synth: Some(synth_preset(SynthPreset::Pad)),
        sample_id: None,
        clips: vec![note_clip("clip-chords", "Chords", 0, 8, chord_notes)],
    };

    let lead_notes = [72u8, 75, 79, 77, 75, 72, 70]
        .into_iter()
        .zip([0usize, 2, 4, 7, 10, 12, 14])
        .enumerate()
        .map(|(i, (note, step))| NoteEvent {
            id: format!("lead-{i}"),
            note,
            start_step: step,
            duration_steps: 1,
            velocity: 88,
        })
        .collect();

    let lead = Track {
        id: "lead".into(),
        name: "Lead".into(),
        kind: TrackKind::Synth,
        mixer: mixer(0.42, 0.0, 0.22, 0.18),
        synth: Some(synth_preset(SynthPreset::Pluck)),
        sample_id: None,
        clips: vec![note_clip("clip-lead", "Lead idea", 4, 4, lead_notes)],
    };

    ProjectState {
        project_id: project_id.to_string(),
        name: "House Jam".into(),
        bpm: 124,
        bars: 8,
        steps_per_bar: 16,
        playing: false,
        start_at_ms: None,
        loop_start_bar: 0,
        loop_end_bar: 8,
        master_volume: 0.85,
        is_public: false,
        revision: 0,
        tracks: vec![drums, bass, chords, lead],
        samples: vec![],
    }
}

fn note_clip(
    id: &str,
    name: &str,
    start_bar: usize,
    length_bars: usize,
    notes: Vec<NoteEvent>,
) -> Clip {
    Clip {
        id: id.into(),
        name: name.into(),
        kind: ClipKind::Notes,
        start_bar,
        length_bars,
        looped: true,
        pattern_steps: 16,
        drum_steps: None,
        notes: Some(notes),
        sample_triggers: None,
    }
}

fn mixer(volume: f32, pan: f32, delay_send: f32, reverb_send: f32) -> MixerState {
    MixerState {
        volume,
        pan,
        muted: false,
        solo: false,
        delay_send,
        reverb_send,
    }
}

fn synth_preset(preset: SynthPreset) -> SynthConfig {
    match preset {
        SynthPreset::DeepBass => SynthConfig {
            preset,
            oscillator: OscillatorWave::Sawtooth,
            attack: 0.005,
            decay: 0.16,
            sustain: 0.45,
            release: 0.12,
            cutoff: 720.0,
            resonance: 5.0,
            detune: 0.0,
        },
        SynthPreset::Acid => SynthConfig {
            preset,
            oscillator: OscillatorWave::Sawtooth,
            attack: 0.002,
            decay: 0.11,
            sustain: 0.22,
            release: 0.08,
            cutoff: 1350.0,
            resonance: 12.0,
            detune: 0.0,
        },
        SynthPreset::Pad => SynthConfig {
            preset,
            oscillator: OscillatorWave::Triangle,
            attack: 0.16,
            decay: 0.35,
            sustain: 0.75,
            release: 0.8,
            cutoff: 2400.0,
            resonance: 2.0,
            detune: -5.0,
        },
        SynthPreset::Pluck => SynthConfig {
            preset,
            oscillator: OscillatorWave::Square,
            attack: 0.002,
            decay: 0.12,
            sustain: 0.18,
            release: 0.09,
            cutoff: 2600.0,
            resonance: 4.0,
            detune: 0.0,
        },
        SynthPreset::Lead => SynthConfig {
            preset,
            oscillator: OscillatorWave::Square,
            attack: 0.008,
            decay: 0.12,
            sustain: 0.62,
            release: 0.15,
            cutoff: 3200.0,
            resonance: 3.5,
            detune: 4.0,
        },
    }
}

fn valid_project_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn safe_id(value: &str) -> String {
    value
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .take(64)
        .collect()
}

fn clean_name(value: &str, max: usize) -> String {
    value
        .chars()
        .filter(|c| !c.is_control())
        .take(max)
        .collect::<String>()
        .trim()
        .to_string()
}

fn safe_extension(filename: &str) -> String {
    FsPath::new(filename)
        .extension()
        .and_then(|ext| ext.to_str())
        .filter(|ext| {
            matches!(
                ext.to_ascii_lowercase().as_str(),
                "wav" | "mp3" | "m4a" | "aac" | "ogg" | "flac" | "webm"
            )
        })
        .map(|ext| ext.to_ascii_lowercase())
        .unwrap_or_else(|| "bin".into())
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
