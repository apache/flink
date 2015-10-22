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

package org.apache.flink.runtime.state.memory;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.util.DataOutputSerializer;
import org.apache.flink.runtime.state.AbstractHeapKvState;

import java.util.HashMap;

/**
 * Heap-backed key/value state that is snapshotted into a serialized memory copy.
 *
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 */
public class MemHeapKvState<K, V> extends AbstractHeapKvState<K, V, MemoryStateBackend> {
	
	public MemHeapKvState(TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer, V defaultValue) {
		super(keySerializer, valueSerializer, defaultValue);
	}

	public MemHeapKvState(TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer,
							V defaultValue, HashMap<K, V> state) {
		super(keySerializer, valueSerializer, defaultValue, state);
	}
	
	@Override
	public MemoryHeapKvStateSnapshot<K, V> shapshot(long checkpointId, long timestamp) throws Exception {
		DataOutputSerializer ser = new DataOutputSerializer(Math.max(size() * 16, 16));
		writeStateToOutputView(ser);
		byte[] bytes = ser.getCopyOfBuffer();
		
		return new MemoryHeapKvStateSnapshot<K, V>(getKeySerializer(), getValueSerializer(), bytes, size());
	}
}
