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

package org.apache.flink.table.api.dataview;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Accessors for {@link DataView}. Some methods and member fields of {@link ListView} and
 * {@link MapView} are internal usage and should be exposed to users. They can only be accessed
 * by this utility but only for internal usage.
 */
@Internal
public class DataViewAccessor {

	/**
	 * Creates an instance of {@link ListView} with the given element data type and the initial list.
	 */
	public static <T> ListView<T> createListView(DataType elementType, List<T> list) {
		return new ListView<>(elementType, list);
	}

	/**
	 * Returns the protected member field {@link ListView#list}.
	 */
	public static <T> List<T> getList(ListView<T> listView) {
		return listView.list;
	}

	/**
	 * Returns the protected member field {@link ListView#elementType}.
	 */
	public static Optional<DataType> getElementType(ListView<?> listView) {
		return Optional.ofNullable(listView.elementType);
	}

	/**
	 * Creates an instance of {@link MapView} with the given key data type and value data
	 * type and the initial map.
	 */
	public static <K, V> MapView<K, V> createMapView(DataType keyType, DataType valueType, Map<K, V> map) {
		return new MapView<>(keyType, valueType, map);
	}

	/**
	 * Returns the protected member field {@link MapView#map}.
	 */
	public static <K, V> Map<K, V> getMap(MapView<K, V> mapView) {
		return mapView.map;
	}

	/**
	 * Returns the protected member field {@link MapView#keyType}.
	 */
	public static Optional<DataType> getKeyType(MapView<?, ?> mapView) {
		return Optional.ofNullable(mapView.keyType);
	}

	/**
	 * Returns the protected member field {@link MapView#valueType}.
	 */
	public static Optional<DataType> getValueType(MapView<?, ?> mapView) {
		return Optional.ofNullable(mapView.valueType);
	}

}
