// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Benchmark different strategies for writing Arrow RecordBatches to disk.
//!
//! 1. `outer-zstd-stream` — one zstd stream wrapping the entire IPC stream
//! 2. `ipc-builtin-zstd` — Arrow IPC StreamWriter with built-in ZSTD compression
//! 3. `per-batch-zstd-stream` — per-batch IPC inside a zstd frame (Comet shuffle style)
//! 4. `per-batch-bulk-zstd` — per-batch IPC buffer, bulk zstd, length-prefixed frames
//! 5. `per-batch-ipc-builtin-zstd` — per-batch IPC stream with built-in ZSTD compression
//!
//! # Usage
//!
//! ```sh
//! cargo run --release --features shuffle-bench --bin ipc_write_bench -- \
//!   --input benchmark_data/customer.parquet \
//!   --output-dir /tmp/ipc_write_bench
//! ```

use std::fs::{self, File};
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::{CompressionType, writer::IpcWriteOptions};
use arrow::ipc::writer::StreamWriter;
use clap::{Parser, ValueEnum};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures::stream::StreamExt;
use zstd::{Decoder, Encoder};

#[derive(Debug, Clone, ValueEnum)]
enum WriteMethod {
    /// Single zstd stream wrapping the entire Arrow IPC stream.
    OuterZstdStream,
    /// Arrow IPC StreamWriter with built-in ZSTD body compression.
    IpcBuiltinZstd,
    /// Per-batch IPC serialized inside an independent zstd frame.
    PerBatchZstdStream,
    /// Per-batch IPC buffer compressed with zstd bulk API and length-prefixed.
    PerBatchBulkZstd,
    /// Per-batch IPC stream with Arrow built-in ZSTD body compression.
    PerBatchIpcBuiltinZstd,
}

#[derive(Parser, Debug)]
#[command(
    name = "ipc_write_bench",
    about = "Benchmark different ColumnBatch / RecordBatch file write strategies"
)]
struct Args {
    /// Path to input Parquet file
    #[arg(long)]
    input: PathBuf,

    /// Directory for benchmark output files
    #[arg(long, default_value = "/tmp/ipc_write_bench")]
    output_dir: PathBuf,

    /// Batch size when scanning Parquet input
    #[arg(long, default_value_t = 8192)]
    batch_size: usize,

    /// Memory limit in bytes for the DataFusion session
    #[arg(long, default_value_t = 2 * 1024 * 1024 * 1024)]
    memory_limit: usize,

    /// Zstd compression level for zstd-based write methods
    #[arg(long, default_value_t = 3)]
    zstd_level: i32,

    /// Write methods to benchmark (default: all)
    #[arg(long, value_enum, num_args = 1..)]
    methods: Vec<WriteMethod>,

    /// Number of timed iterations per method
    #[arg(long, default_value_t = 3)]
    iterations: usize,

    /// Warmup iterations per method (not included in results)
    #[arg(long, default_value_t = 1)]
    warmup: usize,

    /// Verify output by reading rows back after each timed iteration
    #[arg(long, default_value_t = true)]
    verify: bool,

    /// Keep output files after the benchmark finishes
    #[arg(long, default_value_t = false)]
    keep_output: bool,
}

struct BenchResult {
    method: WriteMethod,
    write_times: Vec<f64>,
    read_times: Vec<f64>,
    file_size: u64,
    rows_written: usize,
    rows_read: Option<usize>,
}

fn main() {
    let mut args = Args::parse();
    if args.methods.is_empty() {
        args.methods = vec![
            WriteMethod::OuterZstdStream,
            WriteMethod::IpcBuiltinZstd,
            WriteMethod::PerBatchZstdStream,
            WriteMethod::PerBatchBulkZstd,
            WriteMethod::PerBatchIpcBuiltinZstd,
        ];
    }

    fs::create_dir_all(&args.output_dir).expect("failed to create output directory");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let (schema, batches, total_rows) = rt
        .block_on(load_batches(&args))
        .expect("failed to load input batches");

    println!("=== Column Batch Write Benchmark ===");
    println!("Input:       {}", args.input.display());
    println!("Batches:     {}", batches.len());
    println!("Total rows:  {}", format_number(total_rows));
    println!("Batch size:  {}", format_number(args.batch_size));
    println!("Zstd level:  {}", args.zstd_level);
    println!(
        "Iterations:  {} (warmup: {})",
        args.iterations, args.warmup
    );
    println!();

    let mut results = Vec::new();
    for method in &args.methods {
        let result = run_method_benchmark(
            method.clone(),
            &schema,
            &batches,
            total_rows,
            &args,
        );
        print_method_result(&result);
        results.push(result);
    }

    if results.len() > 1 {
        print_summary_table(&results);
    }

    if !args.keep_output {
        for method in &args.methods {
            let path = output_path(&args.output_dir, method);
            let _ = fs::remove_file(&path);
        }
    }
}

async fn load_batches(
    args: &Args,
) -> datafusion::common::Result<(SchemaRef, Vec<RecordBatch>, usize)> {
    let session_config = SessionConfig::new().with_batch_size(args.batch_size);
    let runtime_env = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_limit(args.memory_limit, 1.0)
            .build()?,
    );
    let ctx = SessionContext::new_with_config_rt(session_config, runtime_env);

    let df = ctx
        .read_parquet(
            args.input.to_str().expect("input path must be valid UTF-8"),
            ParquetReadOptions::default(),
        )
        .await?;
    let schema = df.schema().as_arrow().clone();
    let mut stream = df.execute_stream().await?;

    let mut batches = Vec::new();
    let mut total_rows = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        total_rows += batch.num_rows();
        batches.push(batch);
    }

    Ok((Arc::new(schema), batches, total_rows))
}

fn run_method_benchmark(
    method: WriteMethod,
    schema: &SchemaRef,
    batches: &[RecordBatch],
    total_rows: usize,
    args: &Args,
) -> BenchResult {
    let output_path = output_path(&args.output_dir, &method);
    let total_iters = args.warmup + args.iterations;
    let mut write_times = Vec::with_capacity(args.iterations);
    let mut read_times = Vec::with_capacity(args.iterations);
    let mut last_file_size = 0u64;
    let mut last_rows_read = None;

    for i in 0..total_iters {
        let is_warmup = i < args.warmup;
        let _ = fs::remove_file(&output_path);

        let start = Instant::now();
        write_batches(method.clone(), schema, batches, &output_path, args.zstd_level)
            .expect("write failed");
        let elapsed = start.elapsed().as_secs_f64();

        let file_size = fs::metadata(&output_path).map(|m| m.len()).unwrap_or(0);

        if !is_warmup {
            write_times.push(elapsed);
            last_file_size = file_size;

            if args.verify {
                let read_start = Instant::now();
                let rows_read = verify_output(method.clone(), &output_path, total_rows)
                    .expect("verification failed");
                read_times.push(read_start.elapsed().as_secs_f64());
                last_rows_read = Some(rows_read);
            }
        }
    }

    BenchResult {
        method,
        write_times,
        read_times,
        file_size: last_file_size,
        rows_written: total_rows,
        rows_read: last_rows_read,
    }
}

fn write_batches(
    method: WriteMethod,
    schema: &SchemaRef,
    batches: &[RecordBatch],
    output_path: &Path,
    zstd_level: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    match method {
        WriteMethod::OuterZstdStream => {
            write_outer_zstd_stream(schema, batches, output_path, zstd_level)
        }
        WriteMethod::IpcBuiltinZstd => write_ipc_builtin_zstd(schema, batches, output_path),
        WriteMethod::PerBatchZstdStream => {
            write_per_batch_zstd_stream(schema, batches, output_path, zstd_level)
        }
        WriteMethod::PerBatchBulkZstd => {
            write_per_batch_bulk_zstd(schema, batches, output_path, zstd_level)
        }
        WriteMethod::PerBatchIpcBuiltinZstd => {
            write_per_batch_ipc_builtin_zstd(schema, batches, output_path)
        }
    }
}

/// Method 1: one zstd stream wrapping the entire IPC stream.
fn write_outer_zstd_stream(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    output_path: &Path,
    zstd_level: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create(output_path)?;
    let compression_writer = Encoder::new(file, zstd_level)?;
    let mut ipc_writer = StreamWriter::try_new(compression_writer, schema)?;
    for batch in batches {
        ipc_writer.write(batch)?;
    }
    ipc_writer.finish()?;
    Ok(())
}

/// Method 2: Arrow IPC StreamWriter with built-in ZSTD compression.
fn write_ipc_builtin_zstd(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    output_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create(output_path)?;
    let write_options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::ZSTD))?;
    let mut ipc_writer = StreamWriter::try_new_with_options(file, schema, write_options)?;
    for batch in batches {
        ipc_writer.write(batch)?;
    }
    ipc_writer.finish()?;
    Ok(())
}

/// Method 3: per-batch IPC inside an independent zstd frame.
fn write_per_batch_zstd_stream(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    output_path: &Path,
    zstd_level: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create(output_path)?;
    for batch in batches {
        let encoder = Encoder::new(&file, zstd_level)?;
        let mut arrow_writer = StreamWriter::try_new(encoder, schema)?;
        arrow_writer.write(batch)?;
        arrow_writer.finish()?;
        let zstd_encoder = arrow_writer.into_inner()?;
        zstd_encoder.finish()?;
    }
    file.flush()?;
    Ok(())
}

/// Method 5: per-batch IPC stream with Arrow built-in ZSTD body compression.
fn write_per_batch_ipc_builtin_zstd(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    output_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create(output_path)?;
    let write_options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::ZSTD))?;
    for batch in batches {
        let mut ipc_writer =
            StreamWriter::try_new_with_options(&mut file, schema, write_options.clone())?;
        ipc_writer.write(batch)?;
        ipc_writer.finish()?;
    }
    file.flush()?;
    Ok(())
}

/// Method 4: per-batch IPC buffer, bulk zstd compress, length-prefixed frames.
fn write_per_batch_bulk_zstd(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    output_path: &Path,
    zstd_level: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create(output_path)?;
    for batch in batches {
        let mut ipc_buf = Vec::new();
        let mut arrow_writer = StreamWriter::try_new(&mut ipc_buf, schema)?;
        arrow_writer.write(batch)?;
        arrow_writer.finish()?;

        let compressed = zstd::bulk::compress(&ipc_buf, zstd_level)?;
        let len = compressed.len() as u32;
        file.write_all(&len.to_le_bytes())?;
        file.write_all(&compressed)?;
    }
    file.flush()?;
    Ok(())
}

fn verify_output(
    method: WriteMethod,
    output_path: &Path,
    expected_rows: usize,
) -> Result<usize, Box<dyn std::error::Error>> {
    let rows = match method {
        WriteMethod::OuterZstdStream => verify_zstd_wrapped_ipc_stream(output_path)?,
        WriteMethod::IpcBuiltinZstd => verify_plain_ipc_stream(output_path)?,
        WriteMethod::PerBatchZstdStream => verify_per_batch_zstd_stream(output_path)?,
        WriteMethod::PerBatchBulkZstd => verify_length_prefixed_bulk_zstd(output_path)?,
        WriteMethod::PerBatchIpcBuiltinZstd => verify_concatenated_ipc_streams(output_path)?,
    };

    if rows != expected_rows {
        return Err(format!(
            "row count mismatch: expected {expected_rows}, got {rows}"
        )
        .into());
    }
    Ok(rows)
}

fn verify_zstd_wrapped_ipc_stream(output_path: &Path) -> Result<usize, Box<dyn std::error::Error>> {
    let file = File::open(output_path)?;
    let compression_reader = Decoder::new(file)?;
    count_ipc_rows(compression_reader)
}

/// Each batch is written as an independent zstd frame; scan frame boundaries and
/// decompress one IPC stream per frame.
fn verify_per_batch_zstd_stream(output_path: &Path) -> Result<usize, Box<dyn std::error::Error>> {
    let mut file = File::open(output_path)?;
    let mut compressed = Vec::new();
    file.read_to_end(&mut compressed)?;

    let mut rows = 0usize;
    let mut offset = 0usize;
    while offset < compressed.len() {
        let frame = &compressed[offset..];
        let frame_size = zstd::zstd_safe::find_frame_compressed_size(frame)
            .map_err(|e| format!("failed to locate zstd frame at offset {offset}: {e}"))?;

        let ipc_buf = zstd::decode_all(&compressed[offset..offset + frame_size])?;
        let mut ipc_reader = StreamReader::try_new(Cursor::new(ipc_buf), None)?;
        while let Some(batch) = ipc_reader.next() {
            rows += batch?.num_rows();
        }

        offset += frame_size;
    }
    Ok(rows)
}

fn verify_plain_ipc_stream(output_path: &Path) -> Result<usize, Box<dyn std::error::Error>> {
    let file = File::open(output_path)?;
    count_ipc_rows(file)
}

/// Each batch is written as an independent IPC stream; read them sequentially.
fn verify_concatenated_ipc_streams(output_path: &Path) -> Result<usize, Box<dyn std::error::Error>> {
    let mut file = File::open(output_path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;

    let mut rows = 0usize;
    let mut cursor = Cursor::new(&bytes);
    while (cursor.position() as usize) < bytes.len() {
        let mut ipc_reader = StreamReader::try_new(&mut cursor, None)?;
        while let Some(batch) = ipc_reader.next() { 
            rows += batch?.num_rows();
        }
    }
    Ok(rows)
}

fn verify_length_prefixed_bulk_zstd(output_path: &Path) -> Result<usize, Box<dyn std::error::Error>> {
    let mut file = File::open(output_path)?;
    let mut rows = 0usize;
    loop {
        let mut len_buf = [0u8; 4];
        if file.read_exact(&mut len_buf).is_err() {
            break;
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut compressed = vec![0u8; len];
        file.read_exact(&mut compressed)?;

        let ipc_buf = zstd::decode_all(Cursor::new(compressed))?;
        let mut ipc_reader = StreamReader::try_new(Cursor::new(ipc_buf), None)?;
        while let Some(batch) = ipc_reader.next() {
            rows += batch?.num_rows();
        }
    }
    Ok(rows)
}

fn count_ipc_rows<R: Read>(reader: R) -> Result<usize, Box<dyn std::error::Error>> {
    let mut ipc_reader = StreamReader::try_new(reader, None)?;
    let mut rows = 0usize;
    while let Some(batch) = ipc_reader.next() {
        rows += batch?.num_rows();
    }
    Ok(rows)
}

fn output_path(output_dir: &Path, method: &WriteMethod) -> PathBuf {
    output_dir.join(format!("{}.data", method_file_stem(method)))
}

fn method_file_stem(method: &WriteMethod) -> &'static str {
    match method {
        WriteMethod::OuterZstdStream => "outer_zstd_stream",
        WriteMethod::IpcBuiltinZstd => "ipc_builtin_zstd",
        WriteMethod::PerBatchZstdStream => "per_batch_zstd_stream",
        WriteMethod::PerBatchBulkZstd => "per_batch_bulk_zstd",
        WriteMethod::PerBatchIpcBuiltinZstd => "per_batch_ipc_builtin_zstd",
    }
}

fn method_label(method: &WriteMethod) -> &'static str {
    match method {
        WriteMethod::OuterZstdStream => "outer-zstd-stream",
        WriteMethod::IpcBuiltinZstd => "ipc-builtin-zstd",
        WriteMethod::PerBatchZstdStream => "per-batch-zstd-stream",
        WriteMethod::PerBatchBulkZstd => "per-batch-bulk-zstd",
        WriteMethod::PerBatchIpcBuiltinZstd => "per-batch-ipc-builtin-zstd",
    }
}

fn print_method_result(result: &BenchResult) {
    let avg_write = result.write_times.iter().sum::<f64>() / result.write_times.len() as f64;
    let write_throughput = result.rows_written as f64 / avg_write;

    println!("--- {} ---", method_label(&result.method));
    println!("  write avg:      {:.3}s", avg_write);
    if result.write_times.len() > 1 {
        let min = result
            .write_times
            .iter()
            .cloned()
            .fold(f64::INFINITY, f64::min);
        let max = result
            .write_times
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);
        println!("  write min/max:  {:.3}s / {:.3}s", min, max);
    }
    println!(
        "  write rows/s:   {}",
        format_number(write_throughput as usize)
    );

    if !result.read_times.is_empty() {
        let avg_read = result.read_times.iter().sum::<f64>() / result.read_times.len() as f64;
        let read_throughput = result.rows_written as f64 / avg_read;
        println!("  read avg:       {:.3}s", avg_read);
        if result.read_times.len() > 1 {
            let min = result
                .read_times
                .iter()
                .cloned()
                .fold(f64::INFINITY, f64::min);
            let max = result
                .read_times
                .iter()
                .cloned()
                .fold(f64::NEG_INFINITY, f64::max);
            println!("  read min/max:   {:.3}s / {:.3}s", min, max);
        }
        println!(
            "  read rows/s:    {}",
            format_number(read_throughput as usize)
        );
    }

    println!("  file size:      {}", format_bytes(result.file_size as usize));
    if let Some(rows_read) = result.rows_read {
        println!(
            "  verify rows:    {} ({})",
            format_number(rows_read),
            if rows_read == result.rows_written {
                "ok"
            } else {
                "mismatch"
            }
        );
    }
    println!();
}

fn print_summary_table(results: &[BenchResult]) {
    let has_read = results.iter().any(|r| !r.read_times.is_empty());

    println!("=== Summary ===");
    if has_read {
        println!(
            "{:<28} {:>10} {:>10} {:>12} {:>14} {:>14}",
            "method", "write (s)", "read (s)", "size", "write rows/s", "read rows/s"
        );
        for result in results {
            let avg_write =
                result.write_times.iter().sum::<f64>() / result.write_times.len() as f64;
            let write_throughput = result.rows_written as f64 / avg_write;
            let avg_read = if result.read_times.is_empty() {
                0.0
            } else {
                result.read_times.iter().sum::<f64>() / result.read_times.len() as f64
            };
            let read_throughput = if avg_read > 0.0 {
                result.rows_written as f64 / avg_read
            } else {
                0.0
            };
            println!(
                "{:<28} {:>10.3} {:>10.3} {:>12} {:>14} {:>14}",
                method_label(&result.method),
                avg_write,
                avg_read,
                format_bytes(result.file_size as usize),
                format_number(write_throughput as usize),
                format_number(read_throughput as usize),
            );
        }
    } else {
        println!(
            "{:<28} {:>10} {:>12} {:>14}",
            "method", "write (s)", "size", "write rows/s"
        );
        for result in results {
            let avg_write =
                result.write_times.iter().sum::<f64>() / result.write_times.len() as f64;
            let write_throughput = result.rows_written as f64 / avg_write;
            println!(
                "{:<28} {:>10.3} {:>12} {:>14}",
                method_label(&result.method),
                avg_write,
                format_bytes(result.file_size as usize),
                format_number(write_throughput as usize),
            );
        }
    }
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

fn format_bytes(bytes: usize) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2} MiB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}
