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

import java.util.Map;

/**
 * {@link GenericMapData} is a generic implementation of {@link MapData} which
 * wraps generic Java {@link Map}. All the keys in the map should have the same type
 * and should be in internal data structures, the same to values in the map.
 */
@PublicEvolving
public final class GenericMapData implements MapData {

	private final Map<?, ?> map;

	/**
	 * Creates a {@link GenericMapData} using the given Java Map. The key-value in the map must
	 * be internal data structures.
	 *
	 * @see RowData for more information about internal data structures.
	 */
	public GenericMapData(Map<?, ?> map) {
		this.map = map;
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

