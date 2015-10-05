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

package org.apache.flink.streaming.api.state;

import org.apache.flink.api.common.state.StateCheckpointer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.ByteArrayInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.OutputViewDataOutputStreamWrapper;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of the {@link StateCheckpointer} interface for a map storing
 * types compatible with Flink's serialization system.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class KVMapCheckpointer<K, V> implements StateCheckpointer<HashMap<K, V>, byte[]> {

	private TypeSerializer<K> keySerializer;
	private TypeSerializer<V> valueSerializer;

	public KVMapCheckpointer(TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public byte[] snapshotState(HashMap<K, V> stateMap, long checkpointId, long checkpointTimestamp) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream(stateMap.size() * 16);
		DataOutputView out = new OutputViewDataOutputStreamWrapper(new DataOutputStream(bos));
		try {
			out.writeInt(stateMap.size());
			for (Map.Entry<K, V> kv : stateMap.entrySet()) {
				keySerializer.serialize(kv.getKey(), out);
				valueSerializer.serialize(kv.getValue(), out);
			}
		} catch (IOException e) {
			throw new RuntimeException("Failed to write snapshot", e);
		}
		return bos.toByteArray();
	}

	@Override
	public HashMap<K, V> restoreState(byte[] stateSnapshot) {
		ByteArrayInputView in = new ByteArrayInputView(stateSnapshot);

		HashMap<K, V> returnMap = new HashMap<>();
		try {
			int size = in.readInt();
			for (int i = 0; i < size; i++) {
				returnMap.put(keySerializer.deserialize(in), valueSerializer.deserialize(in));
			}
		} catch (IOException e) {
			throw new RuntimeException("Failed to read snapshot", e);
		}

		return returnMap;
	}
}
