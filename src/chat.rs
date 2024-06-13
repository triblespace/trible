mod util;

#[cfg(feature = "mkl")]
extern crate intel_mkl_src;

#[cfg(feature = "accelerate")]
extern crate accelerate_src;

use clap::Args;
use std::io::Write;
use tokenizers::Tokenizer;

use candle_core::quantized::gguf_file;
use candle_core::Tensor;
use candle_transformers::generation::LogitsProcessor;

use candle_transformers::models::quantized_llama as model;
use model::ModelWeights;
use util::token_output_stream::TokenOutputStream;

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
    let mut tos = TokenOutputStream::new(tokenizer);

    let mut pre_prompt_tokens = vec![];
    for prompt_index in 0.. {
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
        let tokens = tos
            .tokenizer()
            .encode(prompt_str, true)
            .map_err(anyhow::Error::msg)?;
        if args.verbose_prompt {
            for (token, id) in tokens.get_tokens().iter().zip(tokens.get_ids().iter()) {
                let token = token.replace('▁', " ").replace("<0x0A>", "\n");
                println!("{id:7} -> '{token}'");
            }
        }

        let context_tokens = [&pre_prompt_tokens, tokens.get_ids()].concat();
        let mut gen_tokens = vec![];
        let mut logits_processor = LogitsProcessor::new(args.seed, temperature, args.top_p);

        let start_prompt_processing = std::time::Instant::now();

        let context_window_tokens =
            &context_tokens[context_tokens.len().saturating_sub(model::MAX_SEQ_LEN)..];
        let input = Tensor::new(context_window_tokens, &device)?.unsqueeze(0)?;
        let logits = model.forward(&input, 0)?;
        let logits = logits.squeeze(0)?;
        let mut next_token = logits_processor.sample(&logits)?;

        let prompt_dt = start_prompt_processing.elapsed();

        gen_tokens.push(next_token);

        if let Some(t) = tos.next_token(next_token)? {
            print!("{t}");
            std::io::stdout().flush()?;
        }

        let eos_token = "</s>";
        let eos_token = *tos.tokenizer().get_vocab(true).get(eos_token).unwrap();
        let start_post_prompt = std::time::Instant::now();
        let mut sampled = 0;
        for token_index in 0.. {
            let input = Tensor::new(&[next_token], &device)?.unsqueeze(0)?;
            let logits = model.forward(&input, context_window_tokens.len() + token_index)?;
            let logits = logits.squeeze(0)?;
            let logits = if args.repeat_penalty == 1. {
                logits
            } else {
                let start_at = gen_tokens.len().saturating_sub(args.repeat_last_n);
                candle_transformers::utils::apply_repeat_penalty(
                    &logits,
                    args.repeat_penalty,
                    &gen_tokens[start_at..],
                )?
            };
            next_token = logits_processor.sample(&logits)?;
            gen_tokens.push(next_token);
            if let Some(t) = tos.next_token(next_token)? {
                print!("{t}");
                std::io::stdout().flush()?;
            }
            if next_token == eos_token {
                sampled = token_index + 1;
                break;
            };
        }
        if let Some(rest) = tos.decode_rest().map_err(candle_core::Error::msg)? {
            print!("{rest}");
        }
        std::io::stdout().flush()?;
        let dt = start_post_prompt.elapsed();
        println!(
            "\n\n{:4} prompt tokens processed: {:.2} token/s",
            context_tokens.len(),
            context_tokens.len() as f64 / prompt_dt.as_secs_f64(),
        );
        println!(
            "{sampled:4} tokens generated: {:.2} token/s",
            sampled as f64 / dt.as_secs_f64(),
        );

        pre_prompt_tokens = [context_tokens.as_slice(), gen_tokens.as_slice()].concat()
    }

    Ok(())
}
