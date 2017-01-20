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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * A {@link StateDescriptor} for {@link ListState}. This can be used to create a partitioned
 * list state using
 * {@link org.apache.flink.api.common.functions.RuntimeContext#getListState(ListStateDescriptor)}.
 *
 * @param <T> The type of the elements in the list state.
 */
@PublicEvolving
public class ListStateDescriptor<T> extends StateDescriptor<ListState<T>> {
	private static final long serialVersionUID = 1L;

	/** The serializer for the elements in the list. May be eagerly initialized in the constructor,
	 * or lazily once the type is serialized or an ExecutionConfig is provided. */
	private TypeSerializer<T> elemTypeSerializer;

	/** The type information describing the type of the elements. Only used to lazily create the serializer
	 * and dropped during serialization */
	private transient TypeInformation<T> elemTypeInfo;

	/**
	 * Creates a new {@code ListStateDescriptor} with the given name and list element type.
	 *
	 * <p>If this constructor fails (because it is not possible to describe the type via a class),
	 * consider using the {@link #ListStateDescriptor(String, TypeInformation)} constructor.
	 *
	 * @param name The (unique) name for the state.
	 * @param elemTypeClass The type of the elements in the state.
	 */
	public ListStateDescriptor(String name, Class<T> elemTypeClass) {
		super(name);

		try {
			this.elemTypeInfo = TypeExtractor.createTypeInfo(elemTypeClass);
		} catch (Exception e) {
			throw new RuntimeException("Cannot create full type information based on the given class. If the type has generics, please", e);
		}
	}

	/**
	 * Creates a new {@code ListStateDescriptor} with the given name and list element type.
	 *
	 * @param name The (unique) name for the state.
	 * @param elemTypeInfo The type of the elements in the state.
	 */
	public ListStateDescriptor(String name, TypeInformation<T> elemTypeInfo) {
		super(name);

		this.elemTypeInfo = elemTypeInfo;
	}

	/**
	 * Creates a new {@code ListStateDescriptor} with the given name and list element type.
	 *
	 * @param name The (unique) name for the state.
	 * @param elemTypeSerializer The type serializer for the elements in the state.
	 */
	public ListStateDescriptor(String name, TypeSerializer<T> elemTypeSerializer) {
		super(name);

		this.elemTypeSerializer = elemTypeSerializer;
	}
	
	// ------------------------------------------------------------------------

	@Override
	public ListState<T> bind(StateBackend stateBackend) throws Exception {
		return stateBackend.createListState(this);
	}

	/**
	 * Returns the {@link TypeSerializer} that can be used to serialize the elements in the state.
	 * Note that the serializer may initialized lazily and is only guaranteed to exist after
	 * calling {@link #initializeSerializerUnlessSet(ExecutionConfig)}.
	 */
	public TypeSerializer<T> getElemSerializer() {
		if (elemTypeSerializer != null) {
			return elemTypeSerializer;
		} else {
			throw new IllegalStateException("Serializer not yet initialized.");
		}
	}

	@Override
	public boolean isSerializerInitialized() {
		return elemTypeSerializer != null;
	}

	@Override
	public void initializeSerializerUnlessSet(ExecutionConfig executionConfig) {
		if (elemTypeSerializer == null) {
			if (elemTypeInfo != null) {
				elemTypeSerializer = elemTypeInfo.createSerializer(executionConfig);
			} else {
				throw new IllegalStateException(
					"Cannot initialize serializer after TypeInformation was dropped during serialization");
			}
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ListStateDescriptor<?> that = (ListStateDescriptor<?>) o;

		return elemTypeSerializer.equals(that.elemTypeSerializer) && name.equals(that.name);
	}

	@Override
	public int hashCode() {
		int result = elemTypeSerializer.hashCode();
		result = 31 * result + name.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "ListStateDescriptor{" +
				"elem serializer=" + elemTypeSerializer +
				'}';
	}

	// ------------------------------------------------------------------------
	//  Serialization
	// ------------------------------------------------------------------------
	private void writeObject(final ObjectOutputStream out) throws IOException {
		// make sure we have a serializer before the type information gets lost
		initializeSerializerUnlessSet(new ExecutionConfig());

		// write all the non-transient fields
		out.defaultWriteObject();
	}

	@Override
	public Type getType() {
		return Type.LIST;
	}
}
