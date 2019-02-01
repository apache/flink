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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;

import java.util.List;
import java.util.Map;

/**
 * A {@link StateDescriptor} for {@link ListState}. This can be used to create state where the type
 * is a list that can be appended and iterated over.
 *
 * <p>Using {@code ListState} is typically more efficient than manually maintaining a list in a
 * {@link ValueState}, because the backing implementation can support efficient appends, rather than
 * replacing the full list on write.
 *
 * <p>To create keyed list state (on a KeyedStream), use
 * {@link org.apache.flink.api.common.functions.RuntimeContext#getListState(ListStateDescriptor)}.
 *
 * @param <T> The type of the values that can be added to the list state.
 */
@PublicEvolving
public class LargeListStateDescriptor<T> extends StateDescriptor<ListState<T>, Map<Long, List<T>>> {
	private static final long serialVersionUID = 2L;

	/**
	 * Creates a new {@code LargeListStateDescriptor} with the given name and list element type.
	 *
	 * <p>If this constructor fails (because it is not possible to describe the type via a class),
	 * consider using the {@link #LargeListStateDescriptor(String, TypeInformation)} constructor.
	 *
	 * @param name             The (unique) name for the state.
	 * @param elementTypeClass The type of the elements in the state.
	 */
	public LargeListStateDescriptor(String name, Class<T> elementTypeClass) {
		super(name, new MapTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, new ListTypeInfo<T>(elementTypeClass)), null);
	}

	/**
	 * Creates a new {@code LargeListStateDescriptor} with the given name and list element type.
	 *
	 * @param name            The (unique) name for the state.
	 * @param elementTypeInfo The type of the elements in the state.
	 */
	public LargeListStateDescriptor(String name, TypeInformation<T> elementTypeInfo) {
		super(name, new MapTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, new ListTypeInfo<T>(elementTypeInfo)), null);
	}

	/**
	 * Creates a new {@code LargeListStateDescriptor} with the given name and list element type.
	 *
	 * @param name           The (unique) name for the state.
	 * @param typeSerializer The type serializer for the list values.
	 */
	public LargeListStateDescriptor(String name, TypeSerializer<T> typeSerializer) {
		super(name, new MapSerializer<>(LongSerializer.INSTANCE, new ListSerializer<>(typeSerializer)), null);
	}

	/**
	 * Gets the serializer for the elements contained in the list.
	 *
	 * @return The serializer for the elements in the list.
	 */
	public TypeSerializer<T> getElementSerializer() {
		// call getSerializer() here to get the initialization check and proper error message
		final TypeSerializer<Map<Long, List<T>>> rawSerializer = getSerializer();
		if (!(rawSerializer instanceof MapSerializer)) {
			throw new IllegalStateException();
		}

		return ((ListSerializer<T>) ((MapSerializer<Long, List<T>>) rawSerializer).getValueSerializer()).getElementSerializer();
	}

	@Override
	public Type getType() {
		return Type.LIST;
	}
}
