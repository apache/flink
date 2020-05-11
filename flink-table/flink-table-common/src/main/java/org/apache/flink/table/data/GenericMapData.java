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

package org.apache.flink.table.data;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;

import java.util.Map;

/**
 * An internal data structure representing data of {@link MapType} or {@link MultisetType}.
 *
 * <p>{@link GenericMapData} is a generic implementation of {@link MapData} which wraps regular
 * Java maps.
 *
 * <p>Note: All keys and values of this data structure must be internal data structures. All keys must
 * be of the same type; same for values. See {@link RowData} for more information about internal data
 * structures.
 *
 * <p>Both keys and values can contain null for representing nullability.
 */
@PublicEvolving
public final class GenericMapData implements MapData {

	private final Map<?, ?> map;

	/**
	 * Creates an instance of {@link GenericMapData} using the given Java map.
	 *
	 * <p>Note: All keys and values of the map must be internal data structures.
	 */
	public GenericMapData(Map<?, ?> map) {
		this.map = map;
	}

	/**
	 * Returns the value to which the specified key is mapped, or {@code null} if this map
	 * contains no mapping for the key. The returned value is in internal data structure.
	 */
	public Object get(Object key) {
		return map.get(key);
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public ArrayData keyArray() {
		Object[] keys = map.keySet().toArray();
		return new GenericArrayData(keys);
	}

	@Override
	public ArrayData valueArray() {
		Object[] values = map.values().toArray();
		return new GenericArrayData(values);
	}
}

