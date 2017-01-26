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
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

/**
 * Base class for state descriptors. A {@code StateDescriptor} is used for creating
 * {@link State keyed state} in stateful operations. The descriptor contains the name of the state,
 * the serializer to persist the state type, and the configuration that defines the state
 * as queryable.
 * 
 * <h3>Implementation notes</h3>
 * 
 * The actual state object is created given a {@link StateBackend} using the 
 * {@link StateDescriptor#bind(StateBackend)} method.
 * 
 * <p>Subclasses must correctly implement {@link #equals(Object)} and {@link #hashCode()}.
 *
 * @param <S> The type of the State objects created from this {@code StateDescriptor}.
 */
@PublicEvolving
public abstract class StateDescriptor<S extends State> implements Serializable {

	/**
	 * An enumeration of the types of supported states. Used to identify the state type
	 * when writing and restoring checkpoints and savepoints.
	 */
	// IMPORTANT: Do not change the order of the elements in this enum, ordinal is used in serialization
	public enum Type {
		@Deprecated UNKNOWN, VALUE, LIST, REDUCING, FOLDING, AGGREGATING
	}

	private static final long serialVersionUID = 1L;

	// ------------------------------------------------------------------------

	/** Name that uniquely identifies state created from this StateDescriptor. */
	protected final String name;

	/** Name for queries against state created from this StateDescriptor. */
	private String queryableStateName;

	// ------------------------------------------------------------------------

	/**
	 * Create a new {@code StateDescriptor} with the given name and the given type serializer.
	 *
	 * @param name The name of the {@code StateDescriptor}.
	 */
	protected StateDescriptor(String name) {
		this.name = requireNonNull(name, "name must not be null");
	}

	// ------------------------------------------------------------------------
	//  Core State Descriptor Methods 
	// ------------------------------------------------------------------------

	/**
	 * Returns the name of this {@code StateDescriptor}.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Gets the type of the state represented by this descriptor.
	 * @return The state's type
	 */
	public abstract Type getType();

	/**
	 * Creates a new {@link State} on the given {@link StateBackend}.
	 *
	 * @param stateBackend The {@code StateBackend} on which to create the {@link State}.
	 */
	public abstract S bind(StateBackend stateBackend) throws Exception;

	// ------------------------------------------------------------------------
	//  Queryable State Options
	// ------------------------------------------------------------------------

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

	// ------------------------------------------------------------------------
	//  Serializer initialization
	// ------------------------------------------------------------------------

	/**
	 * Checks whether the serializer has been initialized. Serializer initialization is lazy,
	 * to allow parametrization of serializers with an {@link ExecutionConfig} via
	 * {@link #initializeSerializerUnlessSet(ExecutionConfig)}.
	 *
	 * @return True if the serializers have been initialized, false otherwise.
	 */
	public abstract boolean isSerializerInitialized();

	/**
	 * Initializes the serializer, unless it has been initialized before.
	 *
	 * @param executionConfig The execution config to use when creating the serializer.
	 */
	public abstract void initializeSerializerUnlessSet(ExecutionConfig executionConfig);

	// ------------------------------------------------------------------------
	//  Standard Utils
	// ------------------------------------------------------------------------

	@Override
	public abstract int hashCode();

	@Override
	public abstract boolean equals(Object o);

	@Override
	public abstract String toString();
}
