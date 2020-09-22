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

package org.apache.flink.state.api.input;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * An iterator for reading all keys in a state backend across multiple partitioned states.
 *
 * <p>To read unique keys across all partitioned states callers must invoke {@link MultiStateKeyIterator#remove}.
 *
 * @param <K> Type of the key by which state is keyed.
 */
@Internal
public final class MultiStateKeyIterator<K> implements CloseableIterator<K> {
	private final List<? extends StateDescriptor<?, ?>> descriptors;

	private final KeyedStateBackend<K> backend;

	private final Iterator<K> internal;

	private final CloseableRegistry registry;

	private K currentKey;

	public MultiStateKeyIterator(List<? extends StateDescriptor<?, ?>> descriptors, KeyedStateBackend<K> backend) {
		this.descriptors = Preconditions.checkNotNull(descriptors);
		this.backend = Preconditions.checkNotNull(backend);

		this.registry = new CloseableRegistry();
		this.internal = descriptors
			.stream()
			.map(descriptor -> backend.getKeys(descriptor.getName(), VoidNamespace.INSTANCE))
			.peek(stream -> {
				try {
					registry.registerCloseable(stream::close);
				} catch (IOException e) {
					throw new RuntimeException("Failed to read keys from configured StateBackend", e);
				}
			})
			.flatMap(stream -> stream)
			.iterator();
	}

	@Override
	public boolean hasNext() {
		return internal.hasNext();
	}

	@Override
	public K next() {
		currentKey = internal.next();
		return currentKey;
	}

	/** Removes the current key from <b>ALL</b> known states in the state backend. */
	@Override
	public void remove() {
		if (currentKey == null) {
			return;
		}

		for (StateDescriptor<?, ?> descriptor : descriptors) {
			try {
				State state = backend.getPartitionedState(
					VoidNamespace.INSTANCE,
					VoidNamespaceSerializer.INSTANCE,
					descriptor);

				state.clear();
			} catch (Exception e) {
				throw new RuntimeException("Failed to drop partitioned state from state backend", e);
			}
		}
	}

	@Override
	public void close() throws Exception {
		registry.close();
	}
}

