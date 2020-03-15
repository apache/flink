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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A factory that creates for {@link BulkWriter bulk writers} that, when provided
 * with a {@link CompressionCodec}, they compress the data they write. If no codec is
 * provided, the data is written in bulk but uncompressed.
 *
 * @param <IN> The type of element to write.
 */
@PublicEvolving
public class CompressWriterFactory<IN> implements BulkWriter.Factory<IN> {

	private final Extractor<IN> extractor;
	private final Map<String, String> hadoopConfigMap;

	private transient CompressionCodec hadoopCodec;

	private String hadoopCodecName;
	private String codecExtension;

	/**
	 * Creates a new CompressWriterFactory using the given {@link Extractor} to assemble
	 * either {@link HadoopCompressionBulkWriter} or {@link NoCompressionBulkWriter}
	 * based on whether a Hadoop CompressionCodec name is specified.
	 *
	 * @param extractor Extractor to extract the element
	 */
	public CompressWriterFactory(Extractor<IN> extractor) {
		this.extractor = checkNotNull(extractor, "Extractor cannot be null");
		this.hadoopConfigMap = new HashMap<>();
	}

	/**
	 * Compresses the data using the provided Hadoop {@link CompressionCodec}.
	 *
	 * @param codecName Simple/complete name or alias of the CompressionCodec
	 * @return the instance of CompressionWriterFactory
	 * @throws IOException
	 */
	public CompressWriterFactory<IN> withHadoopCompression(String codecName) throws IOException {
		return withHadoopCompression(codecName, new Configuration());
	}

	/**
	 * Compresses the data using the provided Hadoop {@link CompressionCodec} and {@link Configuration}.
	 *
	 * @param codecName Simple/complete name or alias of the CompressionCodec
	 * @param hadoopConfig Hadoop Configuration
	 * @return the instance of CompressionWriterFactory
	 * @throws IOException
	 */
	public CompressWriterFactory<IN> withHadoopCompression(String codecName, Configuration hadoopConfig) throws IOException {
		this.codecExtension = getHadoopCodecExtension(codecName, hadoopConfig);
		this.hadoopCodecName = codecName;

		for (Map.Entry<String, String> entry : hadoopConfig) {
			hadoopConfigMap.put(entry.getKey(), entry.getValue());
		}

		return this;
	}

	@Override
	public BulkWriter<IN> create(FSDataOutputStream out) throws IOException {
		if (hadoopCodecName == null || hadoopCodecName.trim().isEmpty()) {
			return new NoCompressionBulkWriter<>(out, extractor);
		}

		initializeCompressionCodec();

		return new HadoopCompressionBulkWriter<>(hadoopCodec.createOutputStream(out), extractor);
	}

	public String getExtension() {
		return (hadoopCodecName != null) ? this.codecExtension : "";
	}

	private void initializeCompressionCodec() {
		if (hadoopCodec == null) {
			Configuration conf = new Configuration();

			for (Map.Entry<String, String> entry : hadoopConfigMap.entrySet()) {
				conf.set(entry.getKey(), entry.getValue());
			}

			hadoopCodec = new CompressionCodecFactory(conf).getCodecByName(this.hadoopCodecName);
		}
	}

	private String getHadoopCodecExtension(String hadoopCodecName, Configuration conf) throws IOException {
		CompressionCodec codec = new CompressionCodecFactory(conf).getCodecByName(hadoopCodecName);

		if (codec == null) {
			throw new IOException("Unable to load the provided Hadoop codec [" + hadoopCodecName + "]");
		}

		return codec.getDefaultExtension();
	}
}
