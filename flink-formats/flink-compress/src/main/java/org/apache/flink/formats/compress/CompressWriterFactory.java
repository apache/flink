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
 * A factory that creates a {@link BulkWriter} implementation that, when provided
 * with a {@link CompressionCodec} compresses the data and writes. Otherwise simply
 * writes the data in bulk but uncompressed.
 *
 * @param <IN> The type of element to write.
 */
@PublicEvolving
public class CompressWriterFactory<IN> implements BulkWriter.Factory<IN> {

	private Extractor<IN> extractor;
	private CompressionCodec hadoopCodec;
	private String hadoopCodecName;
	private Map<String, String> hadoopConfigurationMap;
	private String codecExtension;

	public CompressWriterFactory(Extractor<IN> extractor) {
		this.extractor = checkNotNull(extractor, "Extractor cannot be null");
		this.hadoopConfigurationMap = new HashMap<>();
	}

	public CompressWriterFactory<IN> withHadoopCompression(String hadoopCodecName) {
		return withHadoopCompression(hadoopCodecName, new Configuration());
	}

	public CompressWriterFactory<IN> withHadoopCompression(String hadoopCodecName, Configuration hadoopConfiguration) {
		CompressionCodec codec = new CompressionCodecFactory(hadoopConfiguration).getCodecByName(hadoopCodecName);
		this.codecExtension = checkNotNull(codec, "Unable to load the provided Hadoop codec [" + hadoopCodecName + "]")
			.getDefaultExtension();

		this.hadoopCodecName = hadoopCodecName;

		for (Map.Entry<String, String> entry : hadoopConfiguration) {
			hadoopConfigurationMap.put(entry.getKey(), entry.getValue());
		}

		return this;
	}

	@Override
	public BulkWriter<IN> create(FSDataOutputStream out) throws IOException {
		if (hadoopCodecName == null || hadoopCodecName.length() == 0) {
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

			for (Map.Entry<String, String> entry : hadoopConfigurationMap.entrySet()) {
				conf.set(entry.getKey(), entry.getValue());
			}

			hadoopCodec = new CompressionCodecFactory(conf).getCodecByName(this.hadoopCodecName);
		}
	}
}
