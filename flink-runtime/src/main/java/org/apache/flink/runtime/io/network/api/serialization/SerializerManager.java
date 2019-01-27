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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.api.common.io.blockcompression.BlockCompressionFactory;
import org.apache.flink.api.common.io.blockcompression.BlockCompressionFactoryLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.partition.BlockingShuffleType;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * {@link SerializerManager} constructs a cascade of serializers or deserializers according to edge configuration.
 * TODO:
 * (1) Here we use properties on {@link SingleInputGate} and TaskManager's configurations to express edge configuration.
 *     It's better to encapsulate an edge's properties.
 * (2) Currently we only support compression, but we can support more transformations on buffers in the same framework
 *     such as encryption.
 * @param <T> The type of the records that are serialized or deserialized.
 */
public class SerializerManager<T extends IOReadableWritable> {
	private static final Logger LOG = LoggerFactory.getLogger(SerializerManager.class);

	/** Whether uses compression during serialization or deserialization. */
	private final boolean useCompression;

	/** Implementation of {@link BlockCompressionFactory} used to create compressors and decompressors. */
	private final Optional<BlockCompressionFactory> compressionFactory;

	/** The max buffer size to compress external shuffle data. */
	private final int compressionBufferSize;

	public SerializerManager(ResultPartitionType resultPartitionType, Configuration configuration) {
		// Conditions to enable compression: (1) the edge is BLOCKING
		//                                && (2) use external shuffle service
		//                                && (3) set TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_ENABLE_COMPRESSION true
		//                                       in flink configuration.
		if (!resultPartitionType.isBlocking()) {
			this.useCompression = false;
		} else {
			BlockingShuffleType shuffleType =
				BlockingShuffleType.getBlockingShuffleTypeFromConfiguration(configuration, LOG);
			this.useCompression = (shuffleType == BlockingShuffleType.YARN) &&
				configuration.getBoolean(TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_ENABLE_COMPRESSION);
		}

		if (!this.useCompression) {
			this.compressionFactory = Optional.empty();
			this.compressionBufferSize = -1;
		} else {
			this.compressionFactory = Optional.of(BlockCompressionFactoryLoader.createBlockCompressionFactory(
				configuration.getString(TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_COMPRESSION_CODEC), configuration));
			this.compressionBufferSize = configuration.getInteger(
				TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_COMPRESSION_BUFFER_SIZE);
		}
	}

	public SerializerManager(SingleInputGate inputGate, Configuration configuration) {
		this(inputGate.getConsumedPartitionType(), configuration);
	}

	public boolean useCompression() {
		return useCompression;
	}

	public RecordDeserializer<T> getRecordDeserializer(String[] tmpDirectories) {
		if (!useCompression) {
			return new SpillingAdaptiveSpanningRecordDeserializer<T>(tmpDirectories);
		} else {
			RecordDeserializer<BufferDeserializationDelegate> internalDeser = new SpillingAdaptiveSpanningRecordDeserializer<>(tmpDirectories);
			BufferDeserializationDelegate internalDeserDelegate = new DecompressionBufferTransformer(compressionFactory.get());
			return new CompositeSpillingAdaptiveSpanningRecordDeserializer<T>(tmpDirectories, internalDeser, internalDeserDelegate);
		}
	}

	public RecordSerializer<IOReadableWritable> getRecordSerializer() {
		if (!useCompression) {
			return new SpanningRecordSerializer<IOReadableWritable>();
		} else {
			SpanningRecordSerializer<BufferSerializationDelegate> internalSer = new SpanningRecordSerializer<BufferSerializationDelegate>();
			BufferSerializationDelegate internalSerDelegate = new CompressionBufferTransformer(compressionFactory.get());
			return new CompositeSpanningRecordSerializer<IOReadableWritable>(internalSer, internalSerDelegate, compressionBufferSize);
		}
	}

}
