/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.common.state;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

/**
 * Base class for state descriptors. A {@code StateDescriptor} is used for creating partitioned
 * {@link State} in stateful operations. This contains the name and can create an actual state
 * object given a {@link StateBackend} using {@link #bind(StateBackend)}.
 *
 * <p>Subclasses must correctly implement {@link #equals(Object)} and {@link #hashCode()}.
 *
 * @param <S> The type of the State objects created from this {@code StateDescriptor}.
 */
public abstract class StateDescriptor<S extends State> implements Serializable {
	private static final long serialVersionUID = 1L;

	/** Name that uniquely identifies state created from this StateDescriptor. */
	protected final String name;

	/**
	 * Create a new {@code StateDescriptor} with the given name.
	 * @param name The name of the {@code StateDescriptor}.
	 */
	public StateDescriptor(String name) {
		this.name = requireNonNull(name);
	}

	/**
	 * Returns the name of this {@code StateDescriptor}.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Creates a new {@link State} on the given {@link StateBackend}.
	 *
	 * @param stateBackend The {@code StateBackend} on which to create the {@link State}.
	 */
	public abstract S bind(StateBackend stateBackend) throws Exception ;

	// Force subclasses to implement
	public abstract boolean equals(Object o);

	// Force subclasses to implement
	public abstract int hashCode();
}
