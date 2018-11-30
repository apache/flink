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

package org.apache.flink.table.dataview;

import org.apache.flink.api.common.typeutils.CompositeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link TypeSerializerSnapshot} for the {@link MapViewSerializer}.
 *
 * @param <K> the key type of the map entries.
 * @param <V> the value type of the map entries.
 */
public class MapViewSerializerSnapshot<K, V> implements TypeSerializerSnapshot<MapView<K, V>> {

	private static final int CURRENT_VERSION = 1;

	private CompositeSerializerSnapshot nestedMapSerializerSnapshot;

	/**
	 * Constructor for read instantiation.
	 */
	public MapViewSerializerSnapshot() {}

	/**
	 * Constructor to create the snapshot for writing.
	 */
	public MapViewSerializerSnapshot(TypeSerializer<Map<K, V>> mapSerializer) {
		this.nestedMapSerializerSnapshot = new CompositeSerializerSnapshot(Preconditions.checkNotNull(mapSerializer));
	}

	@Override
	public int getCurrentVersion() {
		return CURRENT_VERSION;
	}

	@Override
	public TypeSerializer<MapView<K, V>> restoreSerializer() {
		return new MapViewSerializer<>(nestedMapSerializerSnapshot.getRestoreSerializer(0));
	}

	@Override
	public TypeSerializerSchemaCompatibility<MapView<K, V>> resolveSchemaCompatibility(
			TypeSerializer<MapView<K, V>> newSerializer) {
		checkState(nestedMapSerializerSnapshot != null);

		if (newSerializer instanceof MapViewSerializer) {
			MapViewSerializer<K, V> serializer = (MapViewSerializer<K, V>) newSerializer;

			return nestedMapSerializerSnapshot.resolveCompatibilityWithNested(
				TypeSerializerSchemaCompatibility.compatibleAsIs(),
				serializer.getMapSerializer());
		}
		else {
			return TypeSerializerSchemaCompatibility.incompatible();
		}
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		nestedMapSerializerSnapshot.writeCompositeSnapshot(out);
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		this.nestedMapSerializerSnapshot = CompositeSerializerSnapshot.readCompositeSnapshot(in, userCodeClassLoader);
	}
}
