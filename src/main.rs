mod chat;

use anyhow::Result;
use chat::{chat, ChatArgs};
use clap::{Args, Parser, Subcommand};
use rand::{rngs::OsRng, RngCore};
use fast_qr::qr::QRBuilder;

#[derive(Parser)]
#[command(version, about, long_about = None)]
/// A knowledge graph and meta file system for object stores.
///
struct TribleCli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Args, Debug)]
pub struct IdArgs {
    /// Generate a hex string of the ID.
    #[arg(long)]
    hex: bool,
    /// Generate a QR code of the ID.
    #[arg(long)]
    qr: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate a new random id.
    IdGen(IdArgs),
    /// Chat with your knowledge base.
    Chat(ChatArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = TribleCli::parse();
    match args.command {
        Commands::IdGen(args) => {
            let mut id = [0u8; 16];
            OsRng.fill_bytes(&mut id);
            let hex_encoded = hex::encode(id).to_ascii_uppercase();
            if args.hex || !args.qr {
                println!("{}", hex_encoded);
            }
            if args.qr {
                QRBuilder::new(format!("RNDID:{}", hex_encoded.clone()))
                    .version(fast_qr::Version::V02)
                    .mode(fast_qr::Mode::Alphanumeric)
                    .ecl(fast_qr::ECL::M)
                    .build()
                    .unwrap()
                    .print();
            }
        }
        Commands::Chat(args) => {
            chat(args).unwrap();
        }
    }
    Ok(())
}
