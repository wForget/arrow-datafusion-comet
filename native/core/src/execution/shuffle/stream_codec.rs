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

use std::io::{Cursor, Seek, SeekFrom, Write};
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::writer::StreamWriter;
use bytes::Buf;
use crc32fast::Hasher;
use datafusion::physical_plan::metrics::Time;
use simd_adler32::Adler32;
use crate::errors::{CometError, CometResult};

#[derive(Debug, Clone)]
pub enum CompressionCodec {
    None,
    Lz4Frame,
    Zstd(i32),
    Snappy,
}

#[derive(Clone)]
pub struct ShuffleBlockStreamWriter<W: Write> {
    batch_writer: StreamWriter<W>,
}

impl<W: Write> ShuffleBlockStreamWriter<W> {
    pub fn try_new<O: Write> (schema: &Schema, codec: CompressionCodec, output: O) -> datafusion::common::Result<Self> {

        let batch_writer = match &codec {
            CompressionCodec::None => {
                StreamWriter::try_new(output, schema)?
            }
            CompressionCodec::Lz4Frame => {
                let mut wtr = lz4_flex::frame::FrameEncoder::new(output);
                StreamWriter::try_new(&mut wtr, schema)?
            }

            CompressionCodec::Zstd(level) => {
                let encoder = zstd::Encoder::new(output, *level)?;
                StreamWriter::try_new(encoder, schema)?
            }
            CompressionCodec::Snappy => {
                let mut wtr = snap::write::FrameEncoder::new(output);
                StreamWriter::try_new(&mut wtr, schema)?
            }
        };

        Ok(Self {
            batch_writer,
        })
    }

    /// Writes given record batch as Arrow IPC bytes into given writer.
    /// Returns number of bytes written.
    pub fn write_batch(
        &mut self,
        batch: &RecordBatch,
        ipc_time: &Time,
    ) -> datafusion::common::Result<()> {
        if batch.num_rows() == 0 {
            return Ok(())
        }
        let mut timer = ipc_time.timer();
        self.batch_writer.write(batch)?;
        timer.stop();
        Ok(())
    }

    pub fn finish(mut self, write_time: &Time) -> datafusion::common::Result<W> {
        let mut timer = write_time.timer();
        self.batch_writer.finish()?;
        timer.stop();
        Ok(self.batch_writer.into_inner()?)
    }
}

/// Checksum algorithms for writing IPC bytes.
#[derive(Clone)]
pub(crate) enum Checksum {
    /// CRC32 checksum algorithm.
    CRC32(Hasher),
    /// Adler32 checksum algorithm.
    Adler32(Adler32),
}

impl crate::execution::shuffle::codec::Checksum {
    pub(crate) fn try_new(algo: i32, initial_opt: Option<u32>) -> CometResult<Self> {
        match algo {
            0 => {
                let hasher = if let Some(initial) = initial_opt {
                    Hasher::new_with_initial(initial)
                } else {
                    Hasher::new()
                };
                Ok(crate::execution::shuffle::codec::Checksum::CRC32(hasher))
            }
            1 => {
                let hasher = if let Some(initial) = initial_opt {
                    // Note that Adler32 initial state is not zero.
                    // i.e., `Adler32::from_checksum(0)` is not the same as `Adler32::new()`.
                    Adler32::from_checksum(initial)
                } else {
                    Adler32::new()
                };
                Ok(crate::execution::shuffle::codec::Checksum::Adler32(hasher))
            }
            _ => Err(CometError::Internal(
                "Unsupported checksum algorithm".to_string(),
            )),
        }
    }

    pub(crate) fn update(&mut self, cursor: &mut Cursor<&mut Vec<u8>>) -> CometResult<()> {
        match self {
            crate::execution::shuffle::codec::Checksum::CRC32(hasher) => {
                std::io::Seek::seek(cursor, SeekFrom::Start(0))?;
                hasher.update(cursor.chunk());
                Ok(())
            }
            crate::execution::shuffle::codec::Checksum::Adler32(hasher) => {
                std::io::Seek::seek(cursor, SeekFrom::Start(0))?;
                hasher.write(cursor.chunk());
                Ok(())
            }
        }
    }

    pub(crate) fn finalize(self) -> u32 {
        match self {
            crate::execution::shuffle::codec::Checksum::CRC32(hasher) => hasher.finalize(),
            crate::execution::shuffle::codec::Checksum::Adler32(hasher) => hasher.finish(),
        }
    }
}

