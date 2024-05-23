mod chat;

use clap::{Parser, Subcommand};
use anyhow::Result;
use rand::{rngs::OsRng, RngCore};
use chat::{chat, ChatArgs};

#[derive(Parser)]
#[command(version, about, long_about = None)]
/// A knowledge graph and meta file system for object stores.
///
struct TribleCli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate a new random id.
    IdGen{},
    /// Chat with your knowledge base.
    Chat(ChatArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = TribleCli::parse();
    match args.command {
        Commands::IdGen{} => {
            let mut id = [0u8; 16];
            OsRng.fill_bytes(&mut id);
            let encoded_id = hex::encode(id);
            println!("{}", encoded_id.to_ascii_uppercase());
        }
        Commands::Chat(args) => {
            chat(args).unwrap();
        },
    }
    Ok(())
}
