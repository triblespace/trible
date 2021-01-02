use super::trible::Trible;
use blake2s_simd::{Params, State};
use bytes::Buf;
use bytes::Bytes;
use bytes::BytesMut;
use std::convert::TryInto;
use tokio::io;
use tokio_util::codec::Decoder;

/// A transaction is set of tribles atomically added to the log.
#[derive(Debug)]
pub struct Transaction(pub Bytes);

//TODO Add internal state machine enum.
pub struct TransactionCodec {
    hash_state: State,
    txn_hash: Option<[u8; 32]>,
    txn_size: usize,
    in_bad_txn: bool,
}

const ZEROS: [u8; Trible::TXN_ZEROS] = [0; Trible::TXN_ZEROS];

impl Transaction {
    pub fn validate(&self) -> Result<[u8; 32], &'static str> {
        if self.0.len() == 0 {
            return Err("Transaction is empty and doesn't contain an transaction trible.");
        }
        if self.0.len() % Trible::SIZE != 0 {
            return Err("Transaction size needs to be a multiple of trible size.");
        }
        if (self.0)[0..Trible::TXN_ZEROS] != ZEROS {
            return Err("Transaction doesn't start with transaction trible.");
        }
        let hash = &(self.0)[Trible::VALUE_START..Trible::SIZE];
        let mut hash_state = Params::new().hash_length(32).to_state();
        hash_state.update(&(self.0)[Trible::SIZE..]);
        if hash != hash_state.finalize().as_bytes() {
            return Err("Transaction trible hash does not match computed hash.");
        }
        return Ok((&(self.0)[Trible::VALUE_START..Trible::SIZE])
            .try_into()
            .unwrap());
    }
    pub fn try_hash(&self) -> [u8; 32] {
        return (&(self.0)[Trible::VALUE_START..Trible::SIZE])
            .try_into()
            .unwrap();
    }
}

impl TransactionCodec {
    pub fn new() -> TransactionCodec {
        let hash_state = Params::new().hash_length(32).to_state();
        TransactionCodec {
            hash_state,
            txn_hash: None,
            txn_size: 0,
            in_bad_txn: false,
        }
    }
}

impl Decoder for TransactionCodec {
    type Item = Result<Transaction, &'static str>;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            let can_read = self.txn_size + Trible::SIZE <= src.len();
            eprintln!("Decoder: Loop {} {}", src.len(), self.txn_size);
            let at_txn_trible =
                can_read && src[self.txn_size..self.txn_size + Trible::TXN_ZEROS] == ZEROS;
            let at_end = src.len() == 0;
            let in_bad_txn = self.in_bad_txn;
            match self.txn_hash {
                Some(hash) if hash == self.hash_state.finalize().as_bytes() => {
                    eprintln!("Decoder: Has valid.");
                    let txn = Transaction(src.split_to(self.txn_size).freeze());
                    self.hash_state = Params::new().hash_length(32).to_state();
                    self.txn_hash = None;
                    self.txn_size = 0;
                    return Ok(Some(Ok(txn)));
                }
                Some(_) if at_txn_trible => {
                    eprintln!("Decoder: Unexpected tranaction. Invalid hash.");
                    src.advance(self.txn_size);
                    self.hash_state = Params::new().hash_length(32).to_state();
                    self.txn_hash = None;
                    self.txn_size = 0;
                    return Ok(Some(Err(
                        "Unexpected transaction trible while reading transaction. Invalid hash.",
                    )));
                }
                Some(_) if can_read => {
                    eprintln!("Decoder: Updating hash.");
                    self.hash_state
                        .update(&src[self.txn_size..self.txn_size + Trible::SIZE]);
                    self.txn_size += Trible::SIZE;
                }
                None if at_txn_trible && in_bad_txn => {
                    eprintln!("Decoder: At txn start after bad txn.");
                    src.advance(self.txn_size);
                    return Ok(Some(Err(
                        "Transaction doesn't begin with transaction trible.",
                    )));
                }
                None if at_txn_trible => {
                    eprintln!("Decoder: At txn start.");
                    self.txn_hash = Some(
                        (&src[self.txn_size + Trible::VALUE_START..self.txn_size + Trible::SIZE])
                            .try_into()
                            .unwrap(),
                    );
                    self.txn_size = Trible::SIZE;
                }
                None if can_read => {
                    eprintln!("Decoder: Continue consuming invalid txn.");
                    self.in_bad_txn = true;
                    src.advance(Trible::SIZE);
                }
                //TODO do this in decode_eof and check how to keep the thing open...
                None if at_end && in_bad_txn => {
                    eprintln!("Decoder: Empty buffer, while consuming bad txn.");
                    return Ok(Some(Err(
                        "Transaction doesn't begin with transaction trible.",
                    )));
                }
                _ => {
                    eprintln!("Decoder: Default case.");
                    src.reserve(Trible::SIZE);
                    return Ok(None);
                }
            }
        }
    }
}
