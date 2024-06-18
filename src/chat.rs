mod util;

#[cfg(feature = "mkl")]
extern crate intel_mkl_src;

#[cfg(feature = "accelerate")]
extern crate accelerate_src;

use clap::Args;
use tribles::{ Id, NS, Handle, types::hash::Blake3, types::time::NsTAIInterval };
use std::io::Write;
use tokenizers::Tokenizer;

use candle_core::quantized::gguf_file;
use candle_core::Tensor;
use candle_transformers::generation::LogitsProcessor;

use candle_transformers::models::quantized_llama as model;
use model::ModelWeights;
use util::token_output_stream::{ TokenStream, TokenStreamArchive };

NS! {
    pub namespace convo {
        "852D052D37FE8695FCC4F9FD006EC5CC" as tokens: Handle<Blake3, TokenStreamArchive>;
        "A5AF04BC5A055A17B9F77DFDD02847E7" as model: Handle<Blake3, WeightArchive>;
        "C9EEB7B0D65FEB77C0133924C7C4B8F3" as utterance_time: NsTAIInterval;
        "EA495295585C308ED6D02AFAFAE1BB64" as response_to: Id;
        "8BAC1866DBE9B97ABEF6DCBD3FD32163" as speaker: Id;
    }
}

#[derive(Args, Debug)]
pub struct ChatArgs {
    /// The temperature used to generate samples, use 0 for greedy sampling.
    #[arg(long, default_value_t = 0.8)]
    temperature: f64,

    /// Nucleus sampling probability cutoff.
    #[arg(long)]
    top_p: Option<f64>,

    /// The seed to use when generating random samples.
    #[arg(long, default_value_t = 299792458)]
    seed: u64,

    /// Enable tracing (generates a trace-timestamp.json file).
    #[arg(long)]
    tracing: bool,

    /// Display the token for the specified prompt.
    #[arg(long)]
    verbose_prompt: bool,

    /// Penalty to be applied for repeating tokens, 1. means no penalty.
    #[arg(long, default_value_t = 1.1)]
    repeat_penalty: f32,

    /// The context size to consider for the repeat penalty.
    #[arg(long, default_value_t = 64)]
    repeat_last_n: usize,
}

fn format_size(size_in_bytes: usize) -> String {
    if size_in_bytes < 1_000 {
        format!("{}B", size_in_bytes)
    } else if size_in_bytes < 1_000_000 {
        format!("{:.2}KB", size_in_bytes as f64 / 1e3)
    } else if size_in_bytes < 1_000_000_000 {
        format!("{:.2}MB", size_in_bytes as f64 / 1e6)
    } else {
        format!("{:.2}GB", size_in_bytes as f64 / 1e9)
    }
}

pub fn chat(args: ChatArgs) -> anyhow::Result<()> {
    use tracing_chrome::ChromeLayerBuilder;
    use tracing_subscriber::prelude::*;

    let temperature = if args.temperature == 0. {
        None
    } else {
        Some(args.temperature)
    };
    let _guard = if args.tracing {
        let (chrome_layer, guard) = ChromeLayerBuilder::new().build();
        tracing_subscriber::registry().with(chrome_layer).init();
        Some(guard)
    } else {
        None
    };

    println!(
        "avx: {}, neon: {}, simd128: {}, f16c: {}",
        candle_core::utils::with_avx(),
        candle_core::utils::with_neon(),
        candle_core::utils::with_simd128(),
        candle_core::utils::with_f16c()
    );
    println!(
        "temp: {:.2} repeat-penalty: {:.2} repeat-last-n: {}",
        args.temperature, args.repeat_penalty, args.repeat_last_n
    );

    let repo = "TheBloke/Mixtral-8x7B-Instruct-v0.1-GGUF";
    let filename = "mixtral-8x7b-instruct-v0.1.Q6_K.gguf";
    let api = hf_hub::api::sync::Api::new()?;
    let api = api.model(repo.to_string());
    let model_path = api.get(filename)?;

    let mut file = std::fs::File::open(&model_path)?;
    let start = std::time::Instant::now();
    let device = util::device(false)?;

    let model_content = gguf_file::Content::read(&mut file).map_err(|e| e.with_path(model_path))?;
    let mut total_size_in_bytes = 0;
    for (_, tensor) in model_content.tensor_infos.iter() {
        let elem_count = tensor.shape.elem_count();
        total_size_in_bytes +=
            elem_count * tensor.ggml_dtype.type_size() / tensor.ggml_dtype.block_size();
    }
    println!(
        "loaded {:?} tensors ({}) in {:.2}s",
        model_content.tensor_infos.len(),
        &format_size(total_size_in_bytes),
        start.elapsed().as_secs_f32(),
    );
    let mut model = ModelWeights::from_gguf(model_content, &mut file, &device)?;

    println!("model built");

    let api = hf_hub::api::sync::Api::new()?;
    let repo = "mistralai/Mixtral-8x7B-Instruct-v0.1";
    let api = api.model(repo.to_string());
    let tokenizer_path = api.get("tokenizer.json")?;
    let tokenizer = Tokenizer::from_file(tokenizer_path).map_err(anyhow::Error::msg)?;

    let eos_token = "</s>";
    let eos_token = *tokenizer.get_vocab(true).get(eos_token).unwrap();

    let mut tos = TokenStream::new(tokenizer);

    let mut logits_processor = LogitsProcessor::new(args.seed, temperature, args.top_p);

    let mut token_count = 0;
    loop {
        let prompt_str = {
            print!("> ");
            std::io::stdout().flush()?;
            let mut prompt = String::new();
            std::io::stdin().read_line(&mut prompt)?;
            if prompt.ends_with('\n') {
                prompt.pop();
            }
            format!("[INST] {prompt} [/INST]")
        };
        tos.encode(&prompt_str)?;

        let prompt_tokens = tos.archive();

        let start_prompt_processing = std::time::Instant::now();
       
        let mut next_token = 0;
        for token in &prompt_tokens {
            let input = Tensor::new(&[token], &device)?.unsqueeze(0)?;
            let logits = model.forward(&input, token_count)?;
            let logits = logits.squeeze(0)?;
            next_token = logits_processor.sample(&logits)?;
            token_count += 1;
        }

        let prompt_dt = start_prompt_processing.elapsed();

        if let Some(t) = tos.next_token(next_token)? {
            print!("{t}");
            std::io::stdout().flush()?;
        }

        token_count += 1;

        let start_post_prompt = std::time::Instant::now();

        loop {
            let input = Tensor::new(&[next_token], &device)?.unsqueeze(0)?;
            let logits = model.forward(&input, token_count)?;
            let logits = logits.squeeze(0)?;
            let logits = if args.repeat_penalty == 1. {
                logits
            } else {
                tos.apply_repeat_penalty(
                    args.repeat_last_n,
                    args.repeat_penalty,
                    &logits)?
            };
            next_token = logits_processor.sample(&logits)?;
            if let Some(t) = tos.next_token(next_token)? {
                print!("{t}");
                std::io::stdout().flush()?;
            }

            token_count += 1;

            if next_token == eos_token {
                break;
            };
        }

        if let Some(rest) = tos.decode_rest().map_err(candle_core::Error::msg)? {
            print!("{rest}");
        }
        std::io::stdout().flush()?;
        let dt = start_post_prompt.elapsed();

        let gen_tokens = tos.archive();

        println!(
            "\n\n{:4} prompt tokens processed: {:.2} token/s",
            prompt_tokens.len(),
            prompt_tokens.len() as f64 / prompt_dt.as_secs_f64(),
        );
        println!(
            "{:4} tokens generated: {:.2} token/s",
            gen_tokens.len(),
            gen_tokens.len() as f64 / dt.as_secs_f64(),
        );
    }

    Ok(())
}
