use anyhow::{Context, Result};
use bytes::Bytes;
use futures::future;
use futures::stream::{SplitSink, SplitStream, TryStreamExt};
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::path::PathBuf;
use structopt::StructOpt;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncRead;
use tokio::io::{self, AsyncReadExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio_tungstenite::{accept_async, client_async, WebSocketStream};
use tokio_util::codec::{BytesCodec, FramedRead};
use tungstenite::Message;
mod transaction;

#[derive(StructOpt)]
/// A simple but versatile data and knowledge space
///
/// Docs: https://tribles.space/manual
/// Bugs: https://github.com/triblesspace/trible/issues
///
/// To start an archiver:
///   trible archive "new_or_existing_archive.tribles" "wss://localhost:8080"
///
/// To run a notebook attached to a running archiver:
///   trible notebook "wss://localhost:8080"
///
/// To run diagnostics, maintenange and repairs:
///   trible diagnose "some_archive.tribles"
enum TribleCli {
    /// Creates a TribleMQ node that serves as a broker between other nodes.
    /// Persists received Tribles to disk before passing them on.
    Archive {
        #[structopt(parse(from_os_str))]
        write_to: PathBuf,
        serve_on: SocketAddr,
    },
    /// Opens an observable notebook environment connected to the given Trible environment.
    Notebook { connect_to: SocketAddr },
    /// Diagnostics providing analytics, maintenance, and repair tasks.
    Diagnose {},
}

struct Txn<'a> {
    hash: [u8; 32],
    data: &'a [u8],
}

async fn on_incoming(
    addr: SocketAddr,
    incoming: SplitStream<tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>>,
    storage_tx: mpsc::Sender<Txn<'_>>,
) {
    let broadcast_incoming = incoming.try_for_each(|msg| {
        println!(
            "Received a message from {}: {}",
            addr,
            msg.to_text().unwrap()
        );
        future::ok(())
    });
}

async fn on_outgoing(
    addr: SocketAddr,
    outgoing: SplitSink<
        tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        tungstenite::Message,
    >,
    write_to: PathBuf,
    mut latest_txn_rx: watch::Receiver<Option<[u8; 32]>>,
) {
    let mut read_log = OpenOptions::new()
        .write(false)
        .read(true)
        .open(write_to)
        .await
        .unwrap();

    let txn_stream = FramedRead::new(read_log, BytesCodec::new());
    while let Ok(()) = latest_txn_rx.changed().await {
        let latest = (*latest_txn_rx.borrow()).clone();
        match latest {
            None => {}
            Some(hash) => {
                let mut txn = Vec::new();
                let trible = [0; 64];
                read_log.read_exact(&mut trible);
                Message::Binary(txn);
                write_log.write_all(txn.data).await.unwrap();
                write_log.flush().await.unwrap();
                latest_txn_tx.send(Some(txn.hash)).unwrap();
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = TribleCli::from_args();
    match args {
        TribleCli::Archive { write_to, serve_on } => {
            let write_to_storage = write_to.clone();
            let (storage_tx, mut storage_rx) = mpsc::channel::<Txn>(16);
            let (latest_txn_tx, latest_txn_rx) = watch::channel::<Option<[u8; 32]>>(None);
            let mut write_log = OpenOptions::new()
                .create(true)
                .read(false)
                .append(true)
                .open(write_to_storage)
                .await
                .unwrap();
            let storage_task = tokio::spawn(async move {
                while let Some(txn) = storage_rx.recv().await {
                    write_log.write_all(txn.data).await.unwrap();
                    write_log.flush().await.unwrap();
                    latest_txn_tx.send(Some(txn.hash)).unwrap();
                }
            });
            // Create the event loop and TCP listener we'll accept connections on.
            let listener = TcpListener::bind(serve_on).await.unwrap();
            while let Ok((stream, addr)) = listener.accept().await {
                println!("Incoming TCP connection from: {}", addr);
                let ws_stream = tokio_tungstenite::accept_async(stream)
                    .await
                    .expect("Error during the websocket handshake occurred");
                println!("WebSocket connection established: {}", addr);
                let (outgoing, incoming) = ws_stream.split();

                tokio::spawn(on_incoming(addr, incoming, storage_tx.clone()));
                tokio::spawn(on_outgoing(
                    addr,
                    outgoing,
                    write_to.clone(),
                    latest_txn_rx.clone(),
                ));
            }
            storage_task.await.unwrap()
        }
        TribleCli::Notebook { connect_to } => panic!("NOT IMPLEMENTED"),
        TribleCli::Diagnose {} => panic!("NOT IMPLEMENTED"),
    }
    Ok(())
}
