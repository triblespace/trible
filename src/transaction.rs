mod trible;
use tokio_util::codec::Decoder;
use blake2s_simd::{blake2s, Params, Hash, State};


/// A transaction is set of tribles atomically added to the log.
struct Transaction(Vec<u8>)

static zeros: &[u8] = [0; Trible::TXN_ZEROS];

pub struct TransactionCodec {
    hash_state: State,
    txn_hash: Option<[u8; 32]>,
    txn_size: usize;
}

impl TransactionCodec {
    fn new() -> TransactionCodec {
        let state = Params::new()
        .hash_length(32)
        .to_state()
        TransactionCodec {hasher_state: state, txn_hash: None, txn_size: 0}
    }
}

impl Decoder for TransactionCodec {
    type Item = Result<Transaction>;
    type Error = io::Error;
    
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let zeros = [0u8; Self::TXG_ZEROS];
        match self.txn_hash {
            None => {
                while (self.txn_size + Trible:SIZE <= src.len()) {
                    if(&src[self.txn_size..self.txn_size+Trible::TXN_ZEROS] == zeros) {
                        self.txn_hash = Some((&src[txn_size+Trible:VALUE_START..txn_size + Trible:SIZE]).try_into()); // Probably requires copy for borrow checker.
                        if(self.txn_size == 0) {
                            self.txn_size = Trible:SIZE;
                            return Ok(None);
                        } else {
                            src.split_to(self.txn_size);
                            self.txn_size = Trible:SIZE;
                            return Ok(Some(Err("Transaction doesn't begin with transaction trible.")));
                        }
                    } else {
                        self.txn_size += Trible:SIZE;
                    }
                }
            }
            Some(hash) => {
                loop {
                    if(hash == self.hash_state.finalise().as_bytes()) {
                        let txn = Transaction(src.split_to(self.txn_size));
                        self.hash_state = Params::new()
                        .hash_length(32)
                        .to_state()
                        self.txn_hash = None;
                        self.txn_size = 0;
                        return Ok(Some(Ok(txn)));
                    }
                    if !(self.txn_size + Trible:SIZE <= src.len()) {break;}
                    if(&src[self.txn_size..self.txn_size+Trible::TXN_ZEROS] == zeros) {
                        src.split_to(self.txn_size);
                        self.hash_state = Params::new()
                        .hash_length(32)
                        .to_state()
                        self.txn_hash = None;
                        self.txn_size = 0;
                        return Ok(Some(Err("Unexpected transaction trible while reading transaction. Invalid hash.")));
                    } else {
                        self.hash_state.update(&src[txn_size..txn_size + Trible:SIZE]);
                        self.txn_size += Trible:SIZE;
                    }
                }
            }
        }
        return Ok(None);
    }
}