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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * An {@code IdentitySerializerIndex} is a bidirectional map that serves as an index for multiple serializers.
 * It allows lookup for the index of a serializer, and the serializer given a specific index.
 * Same serializer instances are maintained as a single entry in the index.
 *
 * <p>NOTE: Serializing and then deserializing the serializer index essentially results in new serializer
 * instances created for each entry in the index. Therefore, adding the same serializers to the same index
 * after it was serialized would result in new entries.
 */
@Internal
public class IdentitySerializerIndex implements IOReadableWritable {

	/**
	 * Serializer to index directional map. An {@link IdentityHashMap} is
	 * used so that the same serializer instance will only ever be indexed
	 * once and maintained as a single entry.
	 */
	private IdentityHashMap<TypeSerializer<?>, Integer> serializerToIndex;

	/**
	 * Index to serializer directional map.
	 */
	private HashMap<Integer, TypeSerializer<?>> indexToSerializer;

	private int nextAvailableIndex = 0;

	private ClassLoader userCodeClassLoader;

	public IdentitySerializerIndex(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
	}

	public IdentitySerializerIndex() {
		serializerToIndex = new IdentityHashMap<>();
		indexToSerializer = new HashMap<>();
	}

	public void index(TypeSerializer<?> serializer) {
		if (!serializerToIndex.containsKey(serializer)) {
			serializerToIndex.put(serializer, nextAvailableIndex);
			indexToSerializer.put(nextAvailableIndex, serializer);

			nextAvailableIndex++;
		}
	}

	public int getIndexOf(TypeSerializer<?> serializer) {
		if (serializerToIndex.containsKey(serializer)) {
			return serializerToIndex.get(serializer);
		} else {
			return -1;
		}
	}

	public TypeSerializer<?> getSerializer(int index) {
		return indexToSerializer.get(index);
	}

	public boolean containsSerializer(TypeSerializer<?> serializer) {
		return getIndexOf(serializer) != -1;
	}

	public int getSize() {
		return nextAvailableIndex;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		// number of index entries
		out.writeInt(nextAvailableIndex);

		// just need to write one side of the bidirectional map
		for (Map.Entry<TypeSerializer<?>, Integer> entry : serializerToIndex.entrySet()) {
			TypeSerializerSerializationUtil.writeSerializerWithResilience(out, entry.getKey());
			out.writeInt(entry.getValue());
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		nextAvailableIndex = in.readInt();

		serializerToIndex = new IdentityHashMap<>(nextAvailableIndex);
		indexToSerializer = new HashMap<>(nextAvailableIndex);
		for (int i = 0; i < nextAvailableIndex; i++) {
			TypeSerializer<?> serializer = TypeSerializerSerializationUtil.tryReadSerializerWithResilience(in, userCodeClassLoader);
			int index = in.readInt();

			serializerToIndex.put(serializer, index);
			indexToSerializer.put(index, serializer);
		}
	}
}
