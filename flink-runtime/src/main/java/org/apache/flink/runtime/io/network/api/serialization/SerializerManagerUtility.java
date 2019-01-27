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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;

import java.util.HashMap;
import java.util.Map;

/**
 * A helper class to create {@link RecordDeserializer} and {@link RecordSerializer}.
 *
 * @param <T> The type of the records that are serialized or deserialized.
 */
public class SerializerManagerUtility<T extends IOReadableWritable> {

	private final Configuration configuration;

	private final Map<SingleInputGate, SerializerManager<T>> inputGateSerializerManagerMap;

	/** A cache for searching {@link SerializerManager} according to {@link SingleInputGate}
	 *  since {@link InputChannel}s are usually grouped sequentially in an array. */
	private SingleInputGate prevInputGate;
	private SerializerManager<T> prevSerializerManager;

	public SerializerManagerUtility(Configuration configuration) {
		this.configuration = configuration;
		this.inputGateSerializerManagerMap = new HashMap<>();
	}

	/**
	 * Create {@link RecordDeserializer}s according to {@link InputChannel}s in batch
	 * considering input channels of one {@link InputGate} may come from various {@link SingleInputGate}s
	 * with different properties.
	 *
	 * @param inputChannels input channels corresponding to record serializers.
	 * @param tmpDirectories temporary directories for spilling on demand.
	 * @return record deserializers corresponding to input channels.
	 */
	public RecordDeserializer<T>[] createRecordDeserializers(InputChannel[] inputChannels, String[] tmpDirectories) {
		RecordDeserializer<T>[] recordDeserializers = new RecordDeserializer[inputChannels.length];
		for (int i = 0; i < inputChannels.length; i++) {
			recordDeserializers[i] = getOrCreateSerializerManager(inputChannels[i]).getRecordDeserializer(tmpDirectories);
		}
		clear();
		return recordDeserializers;
	}

	private final SerializerManager<T> getOrCreateSerializerManager(InputChannel inputChannel) {
		SingleInputGate currentInputGate = inputChannel.getInputGate();
		if (prevInputGate == currentInputGate) {
			return prevSerializerManager;
		} else {
			// cache miss
			SerializerManager<T> currentSerializerManager = inputGateSerializerManagerMap.get(currentInputGate);
			if (currentSerializerManager == null) {
				currentSerializerManager = new SerializerManager<T>(currentInputGate, configuration);
				inputGateSerializerManagerMap.put(currentInputGate, currentSerializerManager);
			}
			// update cache for searching
			prevInputGate = currentInputGate;
			prevSerializerManager = currentSerializerManager;
			return currentSerializerManager;
		}
	}

	private final void clear() {
		prevInputGate = null;
		prevSerializerManager = null;
		inputGateSerializerManagerMap.clear();
	}
}
