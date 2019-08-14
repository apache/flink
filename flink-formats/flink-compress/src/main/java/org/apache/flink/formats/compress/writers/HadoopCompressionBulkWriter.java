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

package org.apache.flink.formats.compress.writers;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.formats.compress.extractor.Extractor;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.IOException;

/**
 * A {@link BulkWriter} implementation that compresses data using Hadoop codecs.
 *
 * @param <T> The type of element to write.
 */
public class HadoopCompressionBulkWriter<T> implements BulkWriter<T> {

	private Extractor<T> extractor;
	private FSDataOutputStream outputStream;
	private CompressionOutputStream compressor;

	public HadoopCompressionBulkWriter(
			FSDataOutputStream outputStream,
			Extractor<T> extractor,
			CompressionCodec compressionCodec) throws Exception {
		this.outputStream = outputStream;
		this.extractor = extractor;
		this.compressor = compressionCodec.createOutputStream(outputStream);
	}

	@Override
	public void addElement(T element) throws IOException {
		compressor.write(extractor.extract(element));
	}

	@Override
	public void flush() throws IOException {
		compressor.flush();
		outputStream.flush();
	}

	@Override
	public void finish() throws IOException {
		compressor.finish();
		outputStream.sync();
	}
}
