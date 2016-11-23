/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

/**
 * Default implementation of {@link StateInitializationContext}.
 */
public class StateInitializationContextImpl implements StateInitializationContext {

	/** Closable registry to participate in the operator's cancel/close methods */
	private final CloseableRegistry closableRegistry;

	/** Signal whether any state to restore was found */
	private final boolean restored;

	private final OperatorStateStore operatorStateStore;
	private final Collection<OperatorStateHandle> operatorStateHandles;

	private final KeyedStateStore keyedStateStore;
	private final Collection<KeyGroupsStateHandle> keyGroupsStateHandles;

	private final Iterable<KeyGroupStatePartitionStreamProvider> keyedStateIterable;

	public StateInitializationContextImpl(
			boolean restored,
			OperatorStateStore operatorStateStore,
			KeyedStateStore keyedStateStore,
			Collection<KeyGroupsStateHandle> keyGroupsStateHandles,
			Collection<OperatorStateHandle> operatorStateHandles,
			CloseableRegistry closableRegistry) {

		this.restored = restored;
		this.closableRegistry = Preconditions.checkNotNull(closableRegistry);
		this.operatorStateStore = operatorStateStore;
		this.keyedStateStore = keyedStateStore;
		this.operatorStateHandles = operatorStateHandles;
		this.keyGroupsStateHandles = keyGroupsStateHandles;

		this.keyedStateIterable = keyGroupsStateHandles == null ?
				null
				: new Iterable<KeyGroupStatePartitionStreamProvider>() {
			@Override
			public Iterator<KeyGroupStatePartitionStreamProvider> iterator() {
				return new KeyGroupStreamIterator(getKeyGroupsStateHandles().iterator(), getClosableRegistry());
			}
		};
	}

	@Override
	public boolean isRestored() {
		return restored;
	}

	public Collection<OperatorStateHandle> getOperatorStateHandles() {
		return operatorStateHandles;
	}

	public Collection<KeyGroupsStateHandle> getKeyGroupsStateHandles() {
		return keyGroupsStateHandles;
	}

	public CloseableRegistry getClosableRegistry() {
		return closableRegistry;
	}

	@Override
	public Iterable<StatePartitionStreamProvider> getRawOperatorStateInputs() {
		if (null != operatorStateHandles) {
			return new Iterable<StatePartitionStreamProvider>() {
				@Override
				public Iterator<StatePartitionStreamProvider> iterator() {
					return new OperatorStateStreamIterator(
							DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME,
							getOperatorStateHandles().iterator(), getClosableRegistry());
				}
			};
		} else {
			return Collections.emptyList();
		}
	}

	@Override
	public Iterable<KeyGroupStatePartitionStreamProvider> getRawKeyedStateInputs() {
		if(null == keyedStateStore) {
			throw new IllegalStateException("Attempt to access keyed state from non-keyed operator.");
		}

		if (null != keyGroupsStateHandles) {
			return keyedStateIterable;
		} else {
			return Collections.emptyList();
		}
	}

	@Override
	public OperatorStateStore getOperatorStateStore() {
		return operatorStateStore;
	}

	@Override
	public KeyedStateStore getKeyedStateStore() {
		return keyedStateStore;
	}

	public void close() {
		IOUtils.closeQuietly(closableRegistry);
	}

	private static class KeyGroupStreamIterator implements Iterator<KeyGroupStatePartitionStreamProvider> {

		private final Iterator<KeyGroupsStateHandle> stateHandleIterator;
		private final CloseableRegistry closableRegistry;

		private KeyGroupsStateHandle currentStateHandle;
		private FSDataInputStream currentStream;
		private Iterator<Tuple2<Integer, Long>> currentOffsetsIterator;

		public KeyGroupStreamIterator(
				Iterator<KeyGroupsStateHandle> stateHandleIterator, CloseableRegistry closableRegistry) {

			this.stateHandleIterator = Preconditions.checkNotNull(stateHandleIterator);
			this.closableRegistry = Preconditions.checkNotNull(closableRegistry);
		}

		@Override
		public boolean hasNext() {
			if (null != currentStateHandle && currentOffsetsIterator.hasNext()) {
				return true;
			} else {
				while (stateHandleIterator.hasNext()) {
					currentStateHandle = stateHandleIterator.next();
					if (currentStateHandle.getNumberOfKeyGroups() > 0) {
						currentOffsetsIterator = currentStateHandle.getGroupRangeOffsets().iterator();
						closableRegistry.unregisterClosable(currentStream);
						IOUtils.closeQuietly(currentStream);
						currentStream = null;
						return true;
					}
				}
				return false;
			}
		}

		private void openStream() throws IOException {
			FSDataInputStream stream = currentStateHandle.openInputStream();
			closableRegistry.registerClosable(stream);
			currentStream = stream;
		}

		@Override
		public KeyGroupStatePartitionStreamProvider next() {
			Tuple2<Integer, Long> keyGroupOffset = currentOffsetsIterator.next();
			try {
				if (null == currentStream) {
					openStream();
				}
				currentStream.seek(keyGroupOffset.f1);
				return new KeyGroupStatePartitionStreamProvider(currentStream, keyGroupOffset.f0);
			} catch (IOException ioex) {
				return new KeyGroupStatePartitionStreamProvider(ioex, keyGroupOffset.f0);
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Read only Iterator");
		}
	}

	private static class OperatorStateStreamIterator implements Iterator<StatePartitionStreamProvider> {

		private final String stateName; //TODO since we only support a single named state in raw, this could be dropped

		private final Iterator<OperatorStateHandle> stateHandleIterator;
		private final CloseableRegistry closableRegistry;

		private OperatorStateHandle currentStateHandle;
		private FSDataInputStream currentStream;
		private long[] offsets;
		private int offPos;

		public OperatorStateStreamIterator(
				String stateName,
				Iterator<OperatorStateHandle> stateHandleIterator,
				CloseableRegistry closableRegistry) {

			this.stateName = Preconditions.checkNotNull(stateName);
			this.stateHandleIterator = Preconditions.checkNotNull(stateHandleIterator);
			this.closableRegistry = Preconditions.checkNotNull(closableRegistry);
		}

		@Override
		public boolean hasNext() {
			if (null != currentStateHandle && offPos < offsets.length) {
				return true;
			} else {
				while (stateHandleIterator.hasNext()) {
					currentStateHandle = stateHandleIterator.next();
					long[] offsets = currentStateHandle.getStateNameToPartitionOffsets().get(stateName);
					if (null != offsets && offsets.length > 0) {

						this.offsets = offsets;
						this.offPos = 0;

						closableRegistry.unregisterClosable(currentStream);
						IOUtils.closeQuietly(currentStream);
						currentStream = null;

						return true;
					}
				}
				return false;
			}
		}

		private void openStream() throws IOException {
			FSDataInputStream stream = currentStateHandle.openInputStream();
			closableRegistry.registerClosable(stream);
			currentStream = stream;
		}

		@Override
		public StatePartitionStreamProvider next() {
			long offset = offsets[offPos++];
			try {
				if (null == currentStream) {
					openStream();
				}
				currentStream.seek(offset);

				return new StatePartitionStreamProvider(currentStream);
			} catch (IOException ioex) {
				return new StatePartitionStreamProvider(ioex);
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Read only Iterator");
		}
	}
}
