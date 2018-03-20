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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SerializableSupplier;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base class for state descriptors. A {@code StateDescriptor} is used for creating partitioned
 * {@link State} in stateful operations. This contains the name and can create an actual state
 * object given a {@link StateBinder} using {@link #bind(StateBinder)}.
 *
 * <p>Subclasses must correctly implement {@link #equals(Object)} and {@link #hashCode()}.
 *
 * @param <S> The type of the State objects created from this {@code StateDescriptor}.
 * @param <T> The type of the value of the state object described by this state descriptor.
 */
@PublicEvolving
public abstract class StateDescriptor<S extends State, T> implements Serializable {

	/**
	 * An enumeration of the types of supported states. Used to identify the state type
	 * when writing and restoring checkpoints and savepoints.
	 */
	// IMPORTANT: Do not change the order of the elements in this enum, ordinal is used in serialization
	public enum Type {
		/**
		 * @deprecated Enum for migrating from old checkpoints/savepoint versions.
		 */
		@Deprecated
		UNKNOWN,
		VALUE,
		LIST,
		REDUCING,
		FOLDING,
		AGGREGATING,
		MAP
	}

	private static final long serialVersionUID = 1L;

	// ------------------------------------------------------------------------

	/** Name that uniquely identifies state created from this StateDescriptor. */
	private final String name;

	/** The serializer for the type. May be eagerly initialized in the constructor,
	 * or lazily once the type is serialized or an ExecutionConfig is provided. */
	@Nullable
	private TypeSerializer<T> serializer;

	/** The type information describing the value type. Only used to lazily create the serializer
	 * and dropped during serialization */
	@Nullable
	private TypeInformation<T> typeInfo;

	/** Name for queries against state created from this StateDescriptor. */
	@Nullable
	private String queryableStateName;

	/** The default value returned by the state when no other value is bound to a key. */
	@Nullable
	private final SerializableSupplier<T> defaultValueSupplier;

	// ------------------------------------------------------------------------

	/**
	 * Create a new {@code StateDescriptor} with the given name and the given type serializer.
	 *
	 * @param name The name of the {@code StateDescriptor}.
	 * @param serializer The type serializer for the values in the state.
	 * @param defaultValue The supplier for the default value, which is returned when no
	 *                     value has been set previously.
	 */
	protected StateDescriptor(
			String name,
			TypeSerializer<T> serializer,
			@Nullable SerializableSupplier<T> defaultValue) {

		this.name = checkNotNull(name, "name must not be null");
		this.serializer = checkNotNull(serializer, "serializer must not be null");
		this.defaultValueSupplier = defaultValue;
	}

	/**
	 * Create a new {@code StateDescriptor} with the given name and the given type information.
	 *
	 * @param name The name of the {@code StateDescriptor}.
	 * @param typeInfo The type information for the values in the state.
	 * @param defaultValue The supplier for the default value, which is returned when no
	 *                     value has been set previously.
	 */
	protected StateDescriptor(
			String name,
			TypeInformation<T> typeInfo,
			@Nullable SerializableSupplier<T> defaultValue) {

		this.name = checkNotNull(name, "name must not be null");
		this.typeInfo = checkNotNull(typeInfo, "type information must not be null");
		this.defaultValueSupplier = defaultValue;
	}

	/**
	 * Create a new {@code StateDescriptor} with the given name and the given type information.
	 *
	 * <p>If this constructor fails (because it is not possible to describe the type via a class),
	 * consider using the {@link #StateDescriptor(String, TypeInformation, SerializableSupplier)} constructor.
	 *
	 * @param name The name of the {@code StateDescriptor}.
	 * @param type The class of the type of values in the state.
	 * @param defaultValue The supplier for the default value, which is returned when no
	 *                     value has been set previously.
	 */
	protected StateDescriptor(
			String name,
			Class<T> type,
			@Nullable SerializableSupplier<T> defaultValue) {

		this.name = checkNotNull(name, "name must not be null");
		checkNotNull(type, "type class must not be null");

		try {
			this.typeInfo = TypeExtractor.createTypeInfo(type);
		} catch (Exception e) {
			throw new RuntimeException(
					"Could not create the type information for '" + type.getName() + "'. " +
					"The most common reason is failure to infer the generic type information, due to Java's type erasure. " +
					"In that case, please pass a 'TypeHint' instead of a class to describe the type. " +
					"For example, to describe 'Tuple2<String, String>' as a generic type, use " +
					"'new PravegaDeserializationSchema<>(new TypeHint<Tuple2<String, String>>(){}, serializer);'", e);
		}

		this.defaultValueSupplier = defaultValue;
	}

	// ------------------------------------------------------------------------

	/**
	 * Returns the name of this {@code StateDescriptor}.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the default value.
	 */
	public T getDefaultValue() {
		return defaultValueSupplier == null ? null : defaultValueSupplier.get();
	}

	/**
	 * Returns the {@link TypeSerializer} that can be used to serialize the value in the state.
	 * Note that the serializer may initialized lazily and is only guaranteed to exist after
	 * calling {@link #initializeSerializerUnlessSet(ExecutionConfig)}.
	 */
	public TypeSerializer<T> getSerializer() {
		if (serializer != null) {
			return serializer.duplicate();
		} else {
			throw new IllegalStateException("Serializer not yet initialized.");
		}
	}

	/**
	 * Sets the name for queries of state created from this descriptor.
	 *
	 * <p>If a name is set, the created state will be published for queries
	 * during runtime. The name needs to be unique per job. If there is another
	 * state instance published under the same name, the job will fail during runtime.
	 *
	 * @param queryableStateName State name for queries (unique name per job)
	 * @throws IllegalStateException If queryable state name already set
	 */
	public void setQueryable(String queryableStateName) {
		if (this.queryableStateName == null) {
			this.queryableStateName = Preconditions.checkNotNull(queryableStateName, "Registration name");
		} else {
			throw new IllegalStateException("Queryable state name already set");
		}
	}

	/**
	 * Returns the queryable state name.
	 *
	 * @return Queryable state name or <code>null</code> if not set.
	 */
	@Nullable
	public String getQueryableStateName() {
		return queryableStateName;
	}

	/**
	 * Returns whether the state created from this descriptor is queryable.
	 *
	 * @return <code>true</code> if state is queryable, <code>false</code>
	 * otherwise.
	 */
	public boolean isQueryable() {
		return queryableStateName != null;
	}

	/**
	 * Creates a new {@link State} on the given {@link StateBinder}.
	 *
	 * @param stateBinder The {@code StateBackend} on which to create the {@link State}.
	 */
	public abstract S bind(StateBinder stateBinder) throws Exception;

	/**
	 * Gets the type of the state represented by this descriptor, as defined via
	 * the {@link Type} enumeration.
	 */
	public abstract Type getType();

	// ------------------------------------------------------------------------

	/**
	 * Checks whether the serializer has been initialized. Serializer initialization is lazy,
	 * to allow parametrization of serializers with an {@link ExecutionConfig} via
	 * {@link #initializeSerializerUnlessSet(ExecutionConfig)}.
	 *
	 * @return True if the serializers have been initialized, false otherwise.
	 */
	public boolean isSerializerInitialized() {
		return serializer != null;
	}

	/**
	 * Initializes the serializer, unless it has been initialized before.
	 *
	 * @param executionConfig The execution config to use when creating the serializer.
	 */
	public void initializeSerializerUnlessSet(ExecutionConfig executionConfig) {
		if (serializer == null) {
			checkState(typeInfo != null, "no serializer and no type info");

			// instantiate the serializer
			serializer = typeInfo.createSerializer(executionConfig);

			// we can drop the type info now, no longer needed
			typeInfo  = null;
		}
	}

	// ------------------------------------------------------------------------
	//  Standard Utils
	// ------------------------------------------------------------------------

	@Override
	public final int hashCode() {
		return name.hashCode() + 31 * getClass().hashCode();
	}

	@Override
	public final boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		else if (o != null && o.getClass() == this.getClass()) {
			final StateDescriptor<?, ?> that = (StateDescriptor<?, ?>) o;
			return this.name.equals(that.name);
		}
		else {
			return false;
		}
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() +
				"{name=" + name +
				", typeInfo=" + typeInfo +
				", serializer=" + serializer +
				", queryableStateName=" + (isQueryable() ? queryableStateName : "(not queryable)") +
				'}';
	}
}
