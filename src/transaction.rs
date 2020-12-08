use super::trible::Trible;
use blake2s_simd::{blake2s, Hash, Params, State};
use bytes::Bytes;
use bytes::BytesMut;
use std::convert::TryInto;
use tokio::io;
use tokio_util::codec::Decoder;

/// A transaction is set of tribles atomically added to the log.
pub struct Transaction(pub Bytes);

pub struct TransactionCodec {
    hash_state: State,
    txn_hash: Option<[u8; 32]>,
    txn_size: usize,
}

impl Transaction {
    pub fn validate(&self) -> Result<[u8; 32], &'static str> {
        if self.0.len() == 0 {
            return Err("Transaction is empty and doesn't contain an transaction trible.");
        }
        if self.0.len() % Trible::SIZE != 0 {
            return Err("Transaction size needs to be a multiple of trible size.");
        }
        if &(self.0)[0..Trible::TXN_ZEROS] != zeros {
            return Err("Transaction doesn't start with transaction trible.");
        }
        let hash = &(self.0)[Trible::VALUE_START..Trible::SIZE];
        let hash_state = Params::new().hash_length(32).to_state();
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

static zeros: &[u8] = &[0; Trible::TXN_ZEROS][..];

impl TransactionCodec {
    pub fn new() -> TransactionCodec {
        let hash_state = Params::new().hash_length(32).to_state();
        TransactionCodec {
            hash_state,
            txn_hash: None,
            txn_size: 0,
        }
    }
}

impl Decoder for TransactionCodec {
    type Item = Result<Transaction, &'static str>;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.txn_hash {
            None => {
                while self.txn_size + Trible::SIZE <= src.len() {
                    if &src[self.txn_size..self.txn_size + Trible::TXN_ZEROS] == zeros {
                        self.txn_hash = Some(
                            (&src[self.txn_size + Trible::VALUE_START
                                ..self.txn_size + Trible::SIZE])
                                .try_into()
                                .unwrap(),
                        ); // Probably requires copy for borrow checker.
                        if self.txn_size == 0 {
                            self.txn_size = Trible::SIZE;
                            return Ok(None);
                        } else {
                            src.split_to(self.txn_size);
                            self.txn_size = Trible::SIZE;
                            return Ok(Some(Err(
                                "Transaction doesn't begin with transaction trible.",
                            )));
                        }
                    } else {
                        self.txn_size += Trible::SIZE;
                    }
                }
            }
            Some(hash) => {
                loop {
                    if hash == self.hash_state.finalize().as_bytes() {
                        let txn = Transaction(src.split_to(self.txn_size).freeze());
                        self.hash_state = Params::new().hash_length(32).to_state();
                        self.txn_hash = None;
                        self.txn_size = 0;
                        return Ok(Some(Ok(txn)));
                    }
                    if !(self.txn_size + Trible::SIZE <= src.len()) {
                        break;
                    }
                    if &src[self.txn_size..self.txn_size + Trible::TXN_ZEROS] == zeros {
                        src.split_to(self.txn_size);
                        self.hash_state = Params::new().hash_length(32).to_state();
                        self.txn_hash = None;
                        self.txn_size = 0;
                        return Ok(Some(Err("Unexpected transaction trible while reading transaction. Invalid hash.")));
                    } else {
                        self.hash_state
                            .update(&src[self.txn_size..self.txn_size + Trible::SIZE]);
                        self.txn_size += Trible::SIZE;
                    }
                }
            }
        }
        return Ok(None);
    }
}
