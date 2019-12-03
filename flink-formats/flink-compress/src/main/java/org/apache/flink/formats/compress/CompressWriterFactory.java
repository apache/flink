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

package org.apache.flink.formats.compress;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.formats.compress.extractor.Extractor;
import org.apache.flink.formats.compress.writers.HadoopCompressionBulkWriter;
import org.apache.flink.formats.compress.writers.NoCompressionBulkWriter;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;

/**
 * A factory that creates a {@link BulkWriter} implementation that compresses the written data.
 *
 * @param <IN> The type of element to write.
 */
@PublicEvolving
public class CompressWriterFactory<IN> implements BulkWriter.Factory<IN> {

	private Extractor<IN> extractor;
	private CompressionCodec hadoopCodec;

	public CompressWriterFactory(Extractor<IN> extractor) {
		this.extractor = Preconditions.checkNotNull(extractor, "extractor cannot be null");
	}

	public CompressWriterFactory<IN> withHadoopCompression(String hadoopCodecName) {
		return withHadoopCompression(hadoopCodecName, new Configuration());
	}

	public CompressWriterFactory<IN> withHadoopCompression(String hadoopCodecName, Configuration hadoopConfiguration) {
		return withHadoopCompression(new CompressionCodecFactory(hadoopConfiguration).getCodecByName(hadoopCodecName));
	}

	public CompressWriterFactory<IN> withHadoopCompression(CompressionCodec hadoopCodec) {
		this.hadoopCodec = Preconditions.checkNotNull(hadoopCodec, "hadoopCodec cannot be null");
		return this;
	}

	@Override
	public BulkWriter<IN> create(FSDataOutputStream out) throws IOException {
		try {
			return (hadoopCodec != null)
				? new HadoopCompressionBulkWriter<>(out, extractor, hadoopCodec)
				: new NoCompressionBulkWriter<>(out, extractor);
		} catch (Exception e) {
			throw new IOException(e.getLocalizedMessage(), e);
		}
	}

	public String codecExtension() {
		return (hadoopCodec != null)
			? hadoopCodec.getDefaultExtension()
			: "";
	}

}
