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

package org.apache.flink.formats.sequencefile;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.runtime.util.HadoopUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;

/**
 * A factory that creates a SequenceFile {@link BulkWriter}.
 *
 * @param <K> The type of key to write. It should be writable.
 * @param <V> The type of value to write. It should be writable.
 */
@PublicEvolving
public class SequenceFileWriterFactory<K extends Writable, V extends Writable> implements BulkWriter.Factory<Tuple2<K, V>> {
	private static final long serialVersionUID = 1L;

	private final Class<K> keyClass;
	private final Class<V> valueClass;
	private final String compressionCodecName;
	private final SequenceFile.CompressionType compressionType;

	/**
	 * Creates a new SequenceFileWriterFactory using the given builder to assemble the
	 * SequenceFileWriter.
	 *
	 * @param keyClass The class of key to write.
	 * @param valueClass The class of value to write.
	 */
	public SequenceFileWriterFactory(Class<K> keyClass, Class<V> valueClass) {
		this(keyClass, valueClass, "None", SequenceFile.CompressionType.BLOCK);
	}

	/**
	 * Creates a new SequenceFileWriterFactory using the given builder to assemble the
	 * SequenceFileWriter.
	 *
	 * @param keyClass The class of key to write.
	 * @param valueClass The class of value to write.
	 * @param compressionCodecName The name of compression codec.
	 */
	public SequenceFileWriterFactory(Class<K> keyClass, Class<V> valueClass, String compressionCodecName) {
		this(keyClass, valueClass, compressionCodecName, SequenceFile.CompressionType.BLOCK);
	}

	/**
	 * Creates a new SequenceFileWriterFactory using the given builder to assemble the
	 * SequenceFileWriter.
	 *
	 * @param keyClass The class of key to write.
	 * @param valueClass The class of value to write.
	 * @param compressionCodecName The name of compression codec.
	 * @param compressionType The type of compression level.
	 */
	public SequenceFileWriterFactory(Class<K> keyClass, Class<V> valueClass, String compressionCodecName, SequenceFile.CompressionType compressionType) {
		this.keyClass = keyClass;
		this.valueClass = valueClass;
		this.compressionCodecName = compressionCodecName;
		this.compressionType = compressionType;
	}

	@Override
	public BulkWriter<Tuple2<K, V>> create(FSDataOutputStream out) throws IOException {
		Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
		org.apache.hadoop.fs.FSDataOutputStream stream = new org.apache.hadoop.fs.FSDataOutputStream(out, null);
		CompressionCodec compressionCodec = getCompressionCodec(hadoopConf, compressionCodecName);
		SequenceFile.Writer writer = SequenceFile.createWriter(hadoopConf,
			SequenceFile.Writer.stream(stream),
			SequenceFile.Writer.keyClass(keyClass),
			SequenceFile.Writer.valueClass(valueClass),
			SequenceFile.Writer.compression(compressionType, compressionCodec));
		return new SequenceFileWriter(writer);
	}

	private CompressionCodec getCompressionCodec(Configuration conf, String compressionCodecName) {
		CompressionCodec codec = null;
		if (!compressionCodecName.equals("None")) {
			CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
			codec = codecFactory.getCodecByName(compressionCodecName);
			if (codec == null) {
				throw new RuntimeException("Codec " + compressionCodecName + " not found.");
			}
		}
		return codec;
	}
}
