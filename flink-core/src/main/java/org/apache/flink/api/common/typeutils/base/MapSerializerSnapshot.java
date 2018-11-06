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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.CompositeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Snapshot class for the {@link MapSerializer}.
 */
public class MapSerializerSnapshot<K, V> implements TypeSerializerSnapshot<Map<K, V>> {

	private static final int CURRENT_VERSION = 1;

	private CompositeSerializerSnapshot nestedKeyValueSerializerSnapshot;

	/**
	 * Constructor for read instantiation.
	 */
	public MapSerializerSnapshot() {}

	/**
	 * Constructor to create the snapshot for writing.
	 */
	public MapSerializerSnapshot(TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer) {
		Preconditions.checkNotNull(keySerializer);
		Preconditions.checkNotNull(valueSerializer);
		this.nestedKeyValueSerializerSnapshot = new CompositeSerializerSnapshot(keySerializer, valueSerializer);
	}

	@Override
	public int getCurrentVersion() {
		return CURRENT_VERSION;
	}

	@Override
	public TypeSerializer<Map<K, V>> restoreSerializer() {
		return new MapSerializer<>(
			nestedKeyValueSerializerSnapshot.getRestoreSerializer(0),
			nestedKeyValueSerializerSnapshot.getRestoreSerializer(1));
	}

	@Override
	public TypeSerializerSchemaCompatibility<Map<K, V>> resolveSchemaCompatibility(TypeSerializer<Map<K, V>> newSerializer) {
		checkState(nestedKeyValueSerializerSnapshot != null);

		if (newSerializer instanceof MapSerializer) {
			MapSerializer<K, V> serializer = (MapSerializer<K, V>) newSerializer;

			return nestedKeyValueSerializerSnapshot.resolveCompatibilityWithNested(
				TypeSerializerSchemaCompatibility.compatibleAsIs(),
				serializer.getKeySerializer(),
				serializer.getValueSerializer());
		}
		else {
			return TypeSerializerSchemaCompatibility.incompatible();
		}
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		nestedKeyValueSerializerSnapshot.writeCompositeSnapshot(out);
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		this.nestedKeyValueSerializerSnapshot = CompositeSerializerSnapshot.readCompositeSnapshot(in, userCodeClassLoader);
	}
}
