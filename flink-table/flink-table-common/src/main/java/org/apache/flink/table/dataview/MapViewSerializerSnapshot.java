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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.table.api.dataview.MapView;

import java.util.Map;

/**
 * A {@link TypeSerializerSnapshot} for the {@link MapViewSerializer}.
 *
 * @param <K> the key type of the map entries.
 * @param <V> the value type of the map entries.
 */
@Internal
@Deprecated
public class MapViewSerializerSnapshot<K, V> extends CompositeTypeSerializerSnapshot<MapView<K, V>, MapViewSerializer<K, V>> {

	private static final int CURRENT_VERSION = 1;

	/**
	 * Constructor for read instantiation.
	 */
	public MapViewSerializerSnapshot() {
		super(MapViewSerializer.class);
	}

	/**
	 * Constructor to create the snapshot for writing.
	 */
	public MapViewSerializerSnapshot(MapViewSerializer<K, V> mapViewSerializer) {
		super(mapViewSerializer);
	}

	@Override
	public int getCurrentOuterSnapshotVersion() {
		return CURRENT_VERSION;
	}

	@Override
	protected MapViewSerializer<K, V> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
		@SuppressWarnings("unchecked")
		TypeSerializer<Map<K, V>> mapSerializer = (TypeSerializer<Map<K, V>>) nestedSerializers[0];
		return new MapViewSerializer<>(mapSerializer);
	}

	@Override
	protected TypeSerializer<?>[] getNestedSerializers(MapViewSerializer<K, V> outerSerializer) {
		return new TypeSerializer<?>[] { outerSerializer.getMapSerializer() };
	}
}
