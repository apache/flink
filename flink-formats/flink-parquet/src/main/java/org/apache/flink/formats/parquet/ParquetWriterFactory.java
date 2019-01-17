/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;

/**
 * A factory that creates a Parquet {@link BulkWriter}. The factory takes a user-supplied
 * builder to assemble Parquet's writer and then turns it into a Flink {@code BulkWriter}.
 *
 * @param <T> The type of record to write.
 */
@PublicEvolving
public class ParquetWriterFactory<T> implements BulkWriter.Factory<T> {

	private static final long serialVersionUID = 1L;

	private CompressionCodecName compressionCodec = CompressionCodecName.UNCOMPRESSED;

	/** The builder to construct the ParquetWriter. */
	private final ParquetBuilder<T> writerBuilder;

	/**
	 * Creates a new ParquetWriterFactory using the given builder to assemble the
	 * ParquetWriter.
	 *
	 * @param writerBuilder The builder to construct the ParquetWriter.
	 */
	public ParquetWriterFactory(ParquetBuilder<T> writerBuilder) {
		this.writerBuilder = writerBuilder;
	}

	/**
	 * Sets the compression codec to enable compression on the ParquetWriter.
	 *
	 * @param compressionCodec Compression codec
	 */
	public ParquetWriterFactory<T> withCompressionCodec(CompressionCodecName compressionCodec) {
		this.compressionCodec = compressionCodec;
		return this;
	}

	@Override
	public BulkWriter<T> create(FSDataOutputStream stream) throws IOException {
		final OutputFile out = new StreamOutputFile(stream);
		final ParquetWriter<T> writer = writerBuilder.createWriter(out, this.compressionCodec);
		return new ParquetBulkWriter<>(writer);
	}
}
