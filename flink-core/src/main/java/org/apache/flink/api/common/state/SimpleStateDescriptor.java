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

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base class for the descriptors of simple states. Simple state have one value
 * that they store for a key. The counterpart to simple states are states that have composite values.
 *
 * @param <T> The type of the value in the state.
 * @param <S> The type of the created state.
 */
@PublicEvolving
public abstract class SimpleStateDescriptor<T, S extends State> extends StateDescriptor<S> {

	private static final long serialVersionUID = 1L;

	/** The serializer for the type. May be eagerly initialized in the constructor,
	 * or lazily once the type is serialized or an ExecutionConfig is provided. */
	private TypeSerializer<T> typeSerializer;

	/** The type information describing the value type. Only used to lazily create the serializer
	 * and dropped during serialization */
	private transient TypeInformation<T> typeInfo;

	// ------------------------------------------------------------------------

	/**
	 * Create a new {@code SimpleStateDescriptor} with the given name and the given type serializer.
	 *
	 * @param name The name of the {@code SimpleStateDescriptor}.
	 * @param serializer The type serializer for the values in the state.
	 */
	protected SimpleStateDescriptor(String name, TypeSerializer<T> serializer) {
		super(name);
		this.typeSerializer = checkNotNull(serializer, "serializer must not be null");
	}

	/**
	 * Create a new {@code SimpleStateDescriptor} with the given name and the given type information.
	 *
	 * @param name The name of the {@code SimpleStateDescriptor}.
	 * @param typeInfo The type information for the values in the state.
	 */
	protected SimpleStateDescriptor(String name, TypeInformation<T> typeInfo) {
		super(name);
		this.typeInfo = checkNotNull(typeInfo, "type information must not be null");
	}

	/**
	 * Create a new {@code StateDescriptor} with the given name and the given type information.
	 *
	 * <p>If this constructor fails (because it is not possible to describe the type via a class),
	 * consider using the {@link #SimpleStateDescriptor(String, TypeInformation)} constructor.
	 *
	 * @param name The name of the {@code StateDescriptor}.
	 * @param type The class of the type of values in the state.
	 */
	protected SimpleStateDescriptor(String name, Class<T> type) {
		super(name);

		checkNotNull(type, "type class must not be null");

		try {
			this.typeInfo = TypeExtractor.createTypeInfo(type);
		} catch (Exception e) {
			throw new IllegalArgumentException("Cannot create full type information based on the given class. " +
					"If the type has generics, Flink needs to capture the generics. Please use constructor " +
					"StateDescriptor(String name, TypeInformation<T> typeInfo) instead. You can create the" +
					"TypeInformation via the 'TypeInformation.of(TypeHint)' method.", e);
		}
	}

	// ------------------------------------------------------------------------
	//  Serializers
	// ------------------------------------------------------------------------

	/**
	 * Returns the {@link TypeSerializer} that can be used to serialize the value in the state.
	 * Note that the serializer may initialized lazily and is only guaranteed to exist after
	 * calling {@link #initializeSerializerUnlessSet(ExecutionConfig)}.
	 */
	public TypeSerializer<T> getSerializer() {
		checkState(typeSerializer != null, "serializer not yet initialized.");
		return typeSerializer;
	}

	@Override
	public boolean isSerializerInitialized() {
		return typeSerializer != null;
	}

	@Override
	public void initializeSerializerUnlessSet(ExecutionConfig executionConfig) {
		if (typeSerializer == null) {
			checkState(typeInfo != null, "Cannot initialize serializer after TypeInformation was dropped during serialization");
			typeSerializer = typeInfo.createSerializer(executionConfig);
		}
	}

	// ------------------------------------------------------------------------
	//  equals() / hashCode() / toString() helpers
	// ------------------------------------------------------------------------

	protected int simpleStateDescrHashCode() {
		return typeSerializer != null ? typeSerializer.hashCode() : 123;
	}

	protected boolean simpleStateDescrEquals(SimpleStateDescriptor<?, ?> other) {
		return this.typeSerializer == null ? other.typeSerializer == null :
				(other.typeSerializer != null && this.typeSerializer.equals(other.typeSerializer));
	}

	protected String simpleStateDescrToString() {
		return "serializer=" + String.valueOf(typeSerializer); 
	}

	// ------------------------------------------------------------------------
	//  Serialization
	// ------------------------------------------------------------------------

	private void writeObject(final ObjectOutputStream out) throws IOException {
		// make sure we have a serializer before the type information gets lost
		// TODO this is the source of loss of type tags in some user programs that create
		// TODO  the type descriptors on the client side
		initializeSerializerUnlessSet(new ExecutionConfig());

		// write all the non-transient fields
		out.defaultWriteObject();
	}
}
