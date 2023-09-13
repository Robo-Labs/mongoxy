use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn read_mongo_message(stream: &mut TcpStream) -> eyre::Result<(Vec<u8>, u32, u32)> {
    let mut message_length_buf = [0u8; 4];
    stream.read_exact(&mut message_length_buf).await?;
    let message_length = u32::from_le_bytes(message_length_buf);

    let mut request_id_buf = [0u8; 4];
    stream.read_exact(&mut request_id_buf).await?;
    let request_id = u32::from_le_bytes(request_id_buf);

    let mut request_to_buf = [0u8; 4];
    stream.read_exact(&mut request_to_buf).await?;
    let request_to = u32::from_le_bytes(request_to_buf);

    let mut message = vec![0u8; message_length as usize];
    message[0..4].copy_from_slice(&message_length_buf);
    message[4..8].copy_from_slice(&request_id_buf);
    message[8..12].copy_from_slice(&request_to_buf);
    stream.read_exact(&mut message[12..]).await?;
    Ok((message, request_id, request_to))
}

async fn connect_db(db_address: &str, rx: flume::Receiver<MessagePacket>) -> eyre::Result<()> {
    let mut pending_messages = HashMap::new();
    let mut db_stream = TcpStream::connect(db_address).await?;
    loop {
        // select! on messages from rx channel and db_stream
        tokio::select! {
            Ok(message) = rx.recv_async() => {
                db_stream.write_all(&message.message).await?;
                pending_messages.insert(message.request_id, message.tx);
            },
            result = read_mongo_message(&mut db_stream) => {
                match result {
                    Ok((message, _, request_id)) => {
                        if let Some(tx) = pending_messages.remove(&request_id) {
                            tx.send((message, request_id)).unwrap();
                        }
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

fn hash_id(request_id: u32, client_id: &str) -> u32 {
    let mut hasher = DefaultHasher::new();
    client_id.hash(&mut hasher);

    let hash = hasher.finish();

    request_id ^ hash as u32
}

#[derive(Debug)]
struct MessagePacket {
    message: Vec<u8>,
    request_id: u32,
    tx: flume::Sender<(Vec<u8>, u32)>,
}

impl MessagePacket {
    fn new(message: Vec<u8>, request_id: u32, tx: flume::Sender<(Vec<u8>, u32)>) -> Self {
        Self {
            message,
            request_id,
            tx,
        }
    }
}

async fn handle_client(
    mut client: TcpStream,
    db_tx: flume::Sender<MessagePacket>,
    id: String,
) -> eyre::Result<()> {
    let (pending_tx, pending_rx) = flume::unbounded();
    loop {
        tokio::select! {
            result = read_mongo_message(&mut client) => {
                match result {
                    Ok((mut message, request_id, _)) => {
                        let new_request_id = hash_id(request_id, &id);
                        message[4..8].copy_from_slice(&new_request_id.to_le_bytes());
                        let message_packet = MessagePacket::new(message, new_request_id, pending_tx.clone());
                        db_tx.send(message_packet).unwrap();
                    }
                    Err(_) => {
                        break;
                    }
                }
            },
            result = pending_rx.recv_async() => {
                match result {
                    Ok((mut message, request_id)) => {
                        let original_request_id = hash_id(request_id, &id);
                        message[8..12].copy_from_slice(&original_request_id.to_le_bytes());
                        client.write_all(&message).await?;
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let db_address = "127.0.0.1:27017";
    let db_pool_size = 10;

    let (tx, rx) = flume::unbounded::<MessagePacket>();

    for _ in 0..db_pool_size {
        let rx = rx.clone();
        tokio::spawn(connect_db(db_address, rx));
    }

    let listener = TcpListener::bind("127.0.0.1:8080").await?;

    loop {
        let (client, _) = listener.accept().await?;
        let tx = tx.clone();
        let id = client.peer_addr()?.to_string();

        tokio::spawn(handle_client(client, tx, id));
    }
}
