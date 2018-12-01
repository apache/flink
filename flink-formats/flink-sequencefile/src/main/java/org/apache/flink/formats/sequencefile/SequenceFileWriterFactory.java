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
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;

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

	private final SerializableHadoopConfiguration serdeHadoopConf;
	private final Class<K> keyClass;
	private final Class<V> valueClass;
	private final String compressionCodecName;
	private final SequenceFile.CompressionType compressionType;

	/**
	 * Creates a new SequenceFileWriterFactory using the given builder to assemble the
	 * SequenceFileWriter.
	 *
	 * @param hadoopConf The Hadoop configuration for Sequence File Writer.
	 * @param keyClass   The class of key to write.
	 * @param valueClass The class of value to write.
	 */
	public SequenceFileWriterFactory(Configuration hadoopConf, Class<K> keyClass, Class<V> valueClass) {
		this(hadoopConf, keyClass, valueClass, "None", SequenceFile.CompressionType.BLOCK);
	}

	/**
	 * Creates a new SequenceFileWriterFactory using the given builder to assemble the
	 * SequenceFileWriter.
	 *
	 * @param hadoopConf           The Hadoop configuration for Sequence File Writer.
	 * @param keyClass             The class of key to write.
	 * @param valueClass           The class of value to write.
	 * @param compressionCodecName The name of compression codec.
	 */
	public SequenceFileWriterFactory(Configuration hadoopConf, Class<K> keyClass, Class<V> valueClass, String compressionCodecName) {
		this(hadoopConf, keyClass, valueClass, compressionCodecName, SequenceFile.CompressionType.BLOCK);
	}

	/**
	 * Creates a new SequenceFileWriterFactory using the given builder to assemble the
	 * SequenceFileWriter.
	 *
	 * @param hadoopConf           The Hadoop configuration for Sequence File Writer.
	 * @param keyClass             The class of key to write.
	 * @param valueClass           The class of value to write.
	 * @param compressionCodecName The name of compression codec.
	 * @param compressionType      The type of compression level.
	 */
	public SequenceFileWriterFactory(Configuration hadoopConf, Class<K> keyClass, Class<V> valueClass, String compressionCodecName, SequenceFile.CompressionType compressionType) {
		this.serdeHadoopConf = new SerializableHadoopConfiguration(hadoopConf);
		this.keyClass = keyClass;
		this.valueClass = valueClass;
		this.compressionCodecName = compressionCodecName;
		this.compressionType = compressionType;
	}

	@Override
	public BulkWriter<Tuple2<K, V>> create(FSDataOutputStream out) throws IOException {
		org.apache.hadoop.fs.FSDataOutputStream stream = new org.apache.hadoop.fs.FSDataOutputStream(out, null);
		CompressionCodec compressionCodec = getCompressionCodec(serdeHadoopConf.get(), compressionCodecName);
		SequenceFile.Writer writer = SequenceFile.createWriter(serdeHadoopConf.get(),
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

	/**
	 * Get Hadoop configuration based by the path.
	 * If the path is not Hadoop URI, it will be return default configuration.
	 *
	 * @param path The path to get configuration.
	 * @return Hadoop configuration.
	 * @throws IOException
	 */
	public static Configuration getHadoopConfFromPath(Path path) throws IOException {
		FileSystem fs = FileSystem.getUnguardedFileSystem(path.toUri());
		if (fs != null && fs instanceof HadoopFileSystem) {
			return ((HadoopFileSystem) fs).getHadoopFileSystem().getConf();
		} else {
			return new Configuration();
		}
	}
}

