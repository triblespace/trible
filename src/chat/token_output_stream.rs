use candle_core::{Result, Tensor};
use digest::{ Digest, typenum };
use tribles::{types::Hash, BlobParseError, Bloblike, Bytes, Handle};
use zerocopy::AsBytes;
use zerocopy::byteorder;
use zerocopy::LittleEndian;

/// This is a wrapper around a tokenizer to ensure that tokens can be returned to the user in a
/// streaming way rather than having to wait for the full decoding.
pub struct TokenStream {
    tokenizer: tokenizers::Tokenizer,
    tokens: Vec<u32>,
    prev_index: usize,
    current_index: usize,
}

impl TokenStream {
    pub fn new(tokenizer: tokenizers::Tokenizer) -> Self {
        Self {
            tokenizer,
            tokens: Vec::new(),
            prev_index: 0,
            current_index: 0,
        }
    }

    pub fn into_inner(self) -> tokenizers::Tokenizer {
        self.tokenizer
    }

    pub fn encode(&mut self, string: &str) -> anyhow::Result<()> {
        let encodings = self.tokenizer()
            .encode(string, true)
            .map_err(anyhow::Error::msg)?;
        self.tokens.extend_from_slice(encodings.get_ids());
        Ok(())
    }

    pub fn apply_repeat_penalty(&self, context_len: usize, penalty: f32, logits: &Tensor) -> Result<Tensor> {
        let start_at = self.tokens.len().saturating_sub(context_len);
        candle_transformers::utils::apply_repeat_penalty(
            &logits,
            penalty,
            &self.tokens[start_at..]
        )
    }

    fn decode(&self, tokens: &[u32]) -> Result<String> {
        match self.tokenizer.decode(tokens, true) {
            Ok(str) => Ok(str),
            Err(err) => candle_core::bail!("cannot decode: {err}"),
        }
    }

    // https://github.com/huggingface/text-generation-inference/blob/5ba53d44a18983a4de32d122f4cb46f4a17d9ef6/server/text_generation_server/models/model.py#L68
    pub fn next_token(&mut self, token: u32) -> Result<Option<String>> {
        let prev_text = if self.tokens.is_empty() {
            String::new()
        } else {
            let tokens = &self.tokens[self.prev_index..self.current_index];
            self.decode(tokens)?
        };
        self.tokens.push(token);
        let text = self.decode(&self.tokens[self.prev_index..])?;
        if text.len() > prev_text.len() && text.chars().last().unwrap().is_alphanumeric() {
            let text = text.split_at(prev_text.len());
            self.prev_index = self.current_index;
            self.current_index = self.tokens.len();
            Ok(Some(text.1.to_string()))
        } else {
            Ok(None)
        }
    }

    pub fn decode_rest(&self) -> Result<Option<String>> {
        let prev_text = if self.tokens.is_empty() {
            String::new()
        } else {
            let tokens = &self.tokens[self.prev_index..self.current_index];
            self.decode(tokens)?
        };
        let text = self.decode(&self.tokens[self.prev_index..])?;
        if text.len() > prev_text.len() {
            let text = text.split_at(prev_text.len());
            Ok(Some(text.1.to_string()))
        } else {
            Ok(None)
        }
    }

    pub fn decode_all(&self) -> Result<String> {
        self.decode(&self.tokens)
    }

    pub fn get_token(&self, token_s: &str) -> Option<u32> {
        self.tokenizer.get_vocab(true).get(token_s).copied()
    }

    pub fn tokenizer(&self) -> &tokenizers::Tokenizer {
        &self.tokenizer
    }

    pub fn clear(&mut self) {
        self.tokens.clear();
        self.prev_index = 0;
        self.current_index = 0;
    }

    pub fn archive(&mut self) -> TokenStreamArchive {
        self.prev_index = 0;
        self.current_index = 0;

        let mut tokens = std::mem::take(&mut self.tokens);
        tokens.iter_mut().for_each(|i| *i = i.to_be());

        let bytes: Bytes = unsafe {
            // Ensure the original vector is not dropped.
            let mut tokens = std::mem::ManuallyDrop::new(tokens);
            let tokens = Vec::from_raw_parts(tokens.as_mut_ptr() as *mut u8,
                                            tokens.len() * std::mem::size_of::<u32>(),
                                                    tokens.capacity() * std::mem::size_of::<u32>());
            tokens.into()
        };

        TokenStreamArchive(bytes)
    }
}

#[derive(AsBytes)]
#[repr(C)]
pub struct ZCTokenStream([byteorder::U32<LittleEndian>; 1024]);

impl anybytes::ByteOwner for ZCTokenStream {
    fn as_bytes(&self) -> &[u8] {
        AsBytes::as_bytes(self)
    }
}

pub struct TokenStreamArchive(Bytes);

impl TokenStreamArchive {
    pub fn len(&self) -> usize {
        self.0.len() >> 2
    }
}

pub struct TokenStreamArchiveIterator<'a> {
    stream: &'a TokenStreamArchive,
    index: usize,
}

impl<'a> Iterator for TokenStreamArchiveIterator<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.stream.0.len() {
            return None;
        }
        let token = u32::from_be_bytes(self.stream.0[self.index..self.index + std::mem::size_of::<u32>()].try_into().unwrap());
        self.index +=  std::mem::size_of::<u32>();
        Some(token)
    }
}

impl<'a> IntoIterator for &'a TokenStreamArchive {
    type Item = u32;

    type IntoIter = TokenStreamArchiveIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            stream: &self,
            index: 0
        }
    }
}

impl Bloblike for TokenStreamArchive {
    fn into_blob(self) -> Bytes {
        self.0
    }

    fn from_blob(blob: Bytes) -> std::result::Result<Self, tribles::BlobParseError> {
        if (blob.len() % std::mem::size_of::<u32>()) != 0 {
            return Err(BlobParseError::new("failed to load as u32 array"));
        }
        Ok(Self(blob))
    }

    fn as_handle<H>(&self) -> tribles::Handle<H, Self>
    where
        H: Digest<OutputSize = typenum::U32> {
        let digest = H::digest(&self.0);
        unsafe { Handle::new(Hash::new(digest.into())) }
    }
}
