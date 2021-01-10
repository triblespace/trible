pub mod transaction;
pub mod trible;

use anyhow::Result;
use futures::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::path::PathBuf;
use structopt::StructOpt;
use tokio::fs::OpenOptions;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio_util::codec::FramedRead;
use tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tungstenite::http::header::{HeaderValue, SEC_WEBSOCKET_PROTOCOL};
use tungstenite::Message;
use tokio::io::AsyncWriteExt;

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
        /// immediately sync transactions to disk; less fast, more durable
        #[structopt(short, long)]
        sync: bool,
        /// file to write archive to
        #[structopt(parse(from_os_str))]
        write_to: PathBuf,
        /// address and port to listen on
        serve_on: SocketAddr,
    },
    /// Opens an observable notebook environment connected to the given Trible environment.
    Notebook { connect_to: SocketAddr },
    /// Diagnostics providing analytics, maintenance, and repair tasks.
    Diagnose {},
}

async fn on_incoming(
    addr: SocketAddr,
    mut incoming: SplitStream<tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>>,
    storage_tx: mpsc::Sender<transaction::Transaction>,
) {
    eprintln!("Ready to receive txns from {}", addr);
    while let Some(msg) = incoming.next().await {
        eprintln!("Received txn from {}", addr);
        let msg = msg.unwrap();
        let txn = transaction::Transaction(msg.into_data().into());
        match txn.validate() {
            Ok(_) => {
                storage_tx.send(txn).await.unwrap();
            }
            Err(e) => {
                eprintln!("Received bad Transaction from {}: {}", addr, e);
            }
        }
    }
}

async fn on_outgoing(
    addr: SocketAddr,
    mut outgoing: SplitSink<
        tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        tungstenite::Message,
    >,
    write_to: PathBuf,
    mut latest_txn_rx: watch::Receiver<[u8; 32]>,
) {
    eprintln!("Opening log for reading.");
    let read_log = OpenOptions::new()
        .write(false)
        .read(true)
        .open(write_to)
        .await
        .unwrap();

    let mut txn_stream = FramedRead::new(read_log, transaction::TransactionCodec::new());

    while let Some(txn) = txn_stream.next().await {
        match txn.unwrap() {
            //TODO handle file errors.
            Ok(txn) => {
                eprintln!("Read transaction from log.");

                outgoing
                    .send(Message::Binary(txn.0.to_vec()))
                    .await
                    .unwrap();
            }
            Err(e) => {
                eprintln!("Bad Transaction in log: {}", e);
            }
        }
    }
    while let Ok(()) = latest_txn_rx.changed().await {
        eprintln!("New txn notification, writing to {}", addr);
        let hash = { (*latest_txn_rx.borrow()).clone() };
        'log_loop: while let Some(txn) = txn_stream.next().await {
            match txn.unwrap() {
                //TODO handle file errors.
                Ok(txn) => {
                    eprintln!("Read transaction from log.");

                    outgoing
                        .send(Message::Binary(txn.0.to_vec()))
                        .await
                        .unwrap();
                    if hash == txn.try_hash() {
                        break 'log_loop;
                    }
                }
                Err(e) => {
                    eprintln!("Bad Transaction in log: {}", e);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = TribleCli::from_args();
    match args {
        TribleCli::Archive {
            sync,
            write_to,
            serve_on,
        } => {
            let write_to_storage = write_to.clone();
            let (storage_tx, mut storage_rx) = mpsc::channel::<transaction::Transaction>(16);
            let (latest_txn_tx, latest_txn_rx) = watch::channel::<[u8; 32]>([0; 32]);
            eprintln!("Opening log for writing.");
            let mut write_log = OpenOptions::new()
                .create(true)
                .read(false)
                .append(true)
                .open(write_to_storage)
                .await
                .unwrap();
            let _storage_task = tokio::spawn(async move {
                eprintln!("Ready to write to log.");
                while let Some(txn) = storage_rx.recv().await {
                    eprintln!("Writing txn to log.");
                    write_log.write_all(&txn.0[..]).await.unwrap();
                    write_log.flush().await.unwrap();
                    if sync {
                        write_log.sync_all().await.unwrap();
                    }
                    latest_txn_tx.send(txn.try_hash()).unwrap();
                }
            });
            // Create the event loop and TCP listener we'll accept connections on.
            let listener = TcpListener::bind(serve_on).await.unwrap();
            while let Ok((stream, addr)) = listener.accept().await {
                eprintln!("Incoming TCP connection from: {}", addr);
                let ws_stream = tokio_tungstenite::accept_hdr_async(
                    stream,
                    |_request: &Request,
                     mut response: Response|
                     -> Result<Response, ErrorResponse> {
                        response
                            .headers_mut()
                            .append(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static("tribles"));
                        Ok(response)
                    },
                )
                .await
                .expect("Error during the websocket handshake occurred");
                eprintln!("WebSocket connection established: {}", addr);
                let (outgoing, incoming) = ws_stream.split();

                tokio::spawn(on_incoming(addr, incoming, storage_tx.clone()));
                tokio::spawn(on_outgoing(
                    addr,
                    outgoing,
                    write_to.clone(),
                    latest_txn_rx.clone(),
                ));
            }
            //storage_task.await.unwrap()
        }
        TribleCli::Notebook { connect_to: addr } => {
            eprintln!("Can't connect to {} notebooks not implemented yet.", addr);
        }
        TribleCli::Diagnose {} => panic!("NOT IMPLEMENTED"),
    }
    Ok(())
}
