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

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.table.api.dataview.MapView;

/**
 * A {@link TypeSerializerConfigSnapshot} for the {@link MapViewSerializer}.
 *
 * @param <K> the key type of the map entries.
 * @param <V> the value type of the map entries.
 */
public class MapViewSerializerConfigSnapshot<K, V> extends CompositeTypeSerializerConfigSnapshot<MapView<K, V>> {

	private static final int VERSION = 1;

	public MapViewSerializerConfigSnapshot(MapSerializer<K, V> mapSerializer) {
		super(mapSerializer);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected TypeSerializer<MapView<K, V>> restoreSerializer(TypeSerializer<?>[] restoredNestedSerializers) {
		return new MapViewSerializer<>((MapSerializer<K, V>) restoredNestedSerializers[0]);
	}
}
