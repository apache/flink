/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;

/**
 * A {@link Writer} that writes the bucket files as Hadoop {@link SequenceFile SequenceFiles}.
 * The input to the {@link BucketingSink} must
 * be a {@link org.apache.flink.api.java.tuple.Tuple2} of two Hadoop
 * {@link org.apache.hadoop.io.Writable Writables}.
 *
 * @param <K> The type of the first tuple field.
 * @param <V> The type of the second tuple field.
 */
@Deprecated
public class SequenceFileWriter<K extends Writable, V extends Writable> extends StreamWriterBase<Tuple2<K, V>> implements InputTypeConfigurable {
	private static final long serialVersionUID = 1L;

	private final String compressionCodecName;

	private SequenceFile.CompressionType compressionType;

	private transient SequenceFile.Writer writer;

	private Class<K> keyClass;

	private Class<V> valueClass;

	/**
	 * Creates a new {@code SequenceFileWriter} that writes sequence files without compression.
	 */
	public SequenceFileWriter() {
		this("None", SequenceFile.CompressionType.NONE);
	}

	/**
	 * Creates a new {@code SequenceFileWriter} that writes sequence with the given
	 * compression codec and compression type.
	 *
	 * @param compressionCodecName Name of a Hadoop Compression Codec.
	 * @param compressionType The compression type to use.
	 */
	public SequenceFileWriter(String compressionCodecName,
			SequenceFile.CompressionType compressionType) {
		this.compressionCodecName = compressionCodecName;
		this.compressionType = compressionType;
	}

	protected SequenceFileWriter(SequenceFileWriter<K, V> other) {
		super(other);

		this.compressionCodecName = other.compressionCodecName;
		this.compressionType = other.compressionType;
		this.keyClass = other.keyClass;
		this.valueClass = other.valueClass;
	}

	@Override
	public void open(FileSystem fs, Path path) throws IOException {
		super.open(fs, path);
		if (keyClass == null) {
			throw new IllegalStateException("Key Class has not been initialized.");
		}
		if (valueClass == null) {
			throw new IllegalStateException("Value Class has not been initialized.");
		}

		CompressionCodec codec = null;

		Configuration conf = fs.getConf();

		if (!compressionCodecName.equals("None")) {
			CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
			codec = codecFactory.getCodecByName(compressionCodecName);
			if (codec == null) {
				throw new RuntimeException("Codec " + compressionCodecName + " not found.");
			}
		}

		// the non-deprecated constructor syntax is only available in recent hadoop versions...
		writer = SequenceFile.createWriter(conf,
				getStream(),
				keyClass,
				valueClass,
				compressionType,
				codec);
	}

	@Override
	public void close() throws IOException {
		if (writer != null) {
			writer.close();
		}
		super.close();
	}

	@Override
	public void write(Tuple2<K, V> element) throws IOException {
		getStream(); // Throws if the stream is not open
		writer.append(element.f0, element.f1);
	}

	@Override
	public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
		if (!type.isTupleType()) {
			throw new IllegalArgumentException("Input TypeInformation is not a tuple type.");
		}

		TupleTypeInfoBase<?> tupleType = (TupleTypeInfoBase<?>) type;

		if (tupleType.getArity() != 2) {
			throw new IllegalArgumentException("Input TypeInformation must be a Tuple2 type.");
		}

		TypeInformation<K> keyType = tupleType.getTypeAt(0);
		TypeInformation<V> valueType = tupleType.getTypeAt(1);

		this.keyClass = keyType.getTypeClass();
		this.valueClass = valueType.getTypeClass();
	}

	@Override
	public SequenceFileWriter<K, V> duplicate() {
		return new SequenceFileWriter<>(this);
	}

	String getCompressionCodecName() {
		return compressionCodecName;
	}

	SequenceFile.CompressionType getCompressionType() {
		return compressionType;
	}

	Class<K> getKeyClass() {
		return keyClass;
	}

	Class<V> getValueClass() {
		return valueClass;
	}
}
