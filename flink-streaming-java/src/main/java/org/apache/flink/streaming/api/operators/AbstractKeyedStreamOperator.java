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

package org.apache.flink.streaming.api.operators;

import org.apache.commons.io.IOUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Base class for keyed stream operators.
 *
 * <p>For concrete implementations, one of the following two interfaces must also be implemented, to
 * mark the operator as unary or binary:
 * {@link OneInputStreamOperator} or
 * {@link TwoInputStreamOperator}.
 *
 * <p>Methods of {@code StreamOperator} are guaranteed not to be called concurrently. Also, if using
 * the timer service, timer callbacks are also guaranteed not to be called concurrently with
 * methods on {@code StreamOperator}.
 *
 * @param <OUT> The output type of the operator
 */
@Internal
public abstract class AbstractKeyedStreamOperator<K, OUT>
		extends AbstractStreamOperator<OUT>
		implements StreamOperator<OUT>, OutputTypeConfigurable<OUT>, Serializable, KeyContext<K> {

	private static final long serialVersionUID = 1L;

	/** The logger used by the operator class and its subclasses */
	protected static final Logger LOG = LoggerFactory.getLogger(AbstractKeyedStreamOperator.class);

	/** TypeSerializer for our key. */
	private final TypeSerializer<K> keySerializer;

	/** Backend for keyed state. */
	private transient AbstractKeyedStateBackend<K> keyedStateBackend;

	// ---------------- timers ------------------

	protected transient Map<String, HeapInternalTimerService<?, ?>> timerServices;
	protected transient
	Map<String, HeapInternalTimerService.RestoredTimers<?, ?>> restoredServices;
	public AbstractKeyedStreamOperator(TypeSerializer<K> keySerializer) {
		this.keySerializer = requireNonNull(keySerializer);
	}

	public AbstractKeyedStreamOperator(
			Function userFunction,
			TypeSerializer<K> keySerializer) {
		super(userFunction);
		this.keySerializer = keySerializer;
	}

	// ------------------------------------------------------------------------
	//  Life Cycle
	// ------------------------------------------------------------------------

	@Override
	public void open() throws Exception {

		if (timerServices == null) {
			timerServices = new HashMap<>();
		}

		// first initialize the backend in case the user function tries to use it in open()

		KeyGroupRange subTaskKeyGroupRange = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
				container.getEnvironment().getTaskInfo().getNumberOfKeyGroups(),
				container.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks(),
				container.getIndexInSubtaskGroup());

		keyedStateBackend = container.createKeyedStateBackend(
				keySerializer,
				container.getConfiguration().getNumberOfKeyGroups(getUserCodeClassloader()),
				subTaskKeyGroupRange);

		super.open();
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();

		if (keyedStateBackend != null) {
			IOUtils.closeQuietly(keyedStateBackend);
			keyedStateBackend.dispose();
		}
	}

	protected KeyedStateBackend<K> getKeyedStateBackend() {
		return keyedStateBackend;
	}

	@Override
	public void snapshotState(FSDataOutputStream out, long checkpointId, long timestamp) throws Exception {
		super.snapshotState(out, checkpointId, timestamp);

		DataOutputViewStreamWrapper dataOutputView = new DataOutputViewStreamWrapper(out);

		dataOutputView.writeInt(timerServices.size());

		for (Map.Entry<String, HeapInternalTimerService<?, ?>> service : timerServices.entrySet()) {
			dataOutputView.writeUTF(service.getKey());
			service.getValue().snapshotTimers(dataOutputView);
		}

	}

	@Override
	public void restoreState(FSDataInputStream in) throws Exception {
		super.restoreState(in);

		DataInputViewStreamWrapper dataInputView = new DataInputViewStreamWrapper(in);

		restoredServices = new HashMap<>();

		int numServices = dataInputView.readInt();

		for (int i = 0; i < numServices; i++) {
			String name = dataInputView.readUTF();
			HeapInternalTimerService.RestoredTimers restoredService =
					new HeapInternalTimerService.RestoredTimers(in, getUserCodeClassloader());
			restoredServices.put(name, restoredService);
		}

	}

	/**
	 * Returns a {@link InternalTimerService} that can be used to query current processing time
	 * and event time and to set timers. An operator can have several timer services, where
	 * each has its own namespace serializer. Timer services are differentiated by the string
	 * key that is given when requesting them, if you call this method with the same key
	 * multiple times you will get the same timer service instance in subsequent requests.
	 *
	 * <p>Timers are always scoped to a key, the currently active key of a keyed stream operation.
	 * When a timer fires, this key will also be set as the currently active key.
	 *
	 * <p>Each timer has attached metadata, the namespace. Different timer services
	 * can have a different namespace type. If you don't need namespace differentiation you
	 * can use {@link VoidNamespaceSerializer} as the namespace serializer.
	 *
	 * @param name The name of the requested timer service. If no service exists under the given
	 *             name a new one will be created and returned.
	 * @param keySerializer {@code TypeSerializer} for the keys of the timers.
	 * @param namespaceSerializer {@code TypeSerializer} for the timer namespace.
	 * @param triggerable The {@link Triggerable} that should be invoked when timers fire
	 *
	 * @param <K> The type of the timer keys.
	 * @param <N> The type of the timer namespace.
	 */
	public <K, N> InternalTimerService<N> getInternalTimerService(
			String name,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			Triggerable<K, N> triggerable) {

		@SuppressWarnings("unchecked")
		HeapInternalTimerService<K, N> service = (HeapInternalTimerService<K, N>) timerServices.get(name);

		if (service == null) {
			if (restoredServices != null && restoredServices.containsKey(name)) {
				@SuppressWarnings("unchecked")
				HeapInternalTimerService.RestoredTimers<K, N> restoredService =
						(HeapInternalTimerService.RestoredTimers<K, N>) restoredServices.remove(name);

				service = new HeapInternalTimerService<>(
						keySerializer,
						namespaceSerializer,
						triggerable,
						this,
						getRuntimeContext().getProcessingTimeService(),
						restoredService);

			} else {
				service = new HeapInternalTimerService<>(
						keySerializer,
						namespaceSerializer,
						triggerable,
						this,
						getRuntimeContext().getProcessingTimeService());
			}
			timerServices.put(name, service);
		}

		return service;
	}


	/**
	 * Creates a partitioned state handle, using the state backend configured for this task.
	 * 
	 * @throws IllegalStateException Thrown, if the key/value state was already initialized.
	 * @throws Exception Thrown, if the state backend cannot create the key/value state.
	 */
	@Override
	protected <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) throws Exception {
		return getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
	}

	/**
	 * Creates a partitioned state handle, using the state backend configured for this task.
	 *
	 * @throws IllegalStateException Thrown, if the key/value state was already initialized.
	 * @throws Exception Thrown, if the state backend cannot create the key/value state.
	 */
	@Override
	protected <S extends State, N> S getPartitionedState(
			N namespace,
			TypeSerializer<N> namespaceSerializer,
			StateDescriptor<S, ?> stateDescriptor) throws Exception {
		return keyedStateBackend.getPartitionedState(
				namespace,
				namespaceSerializer,
				stateDescriptor);
	}

	@Override
	public void setCurrentKey(K key) {
		keyedStateBackend.setCurrentKey(key);
	}

	@Override
	public K getCurrentKey() {
		return keyedStateBackend.getCurrentKey();
	}

	@VisibleForTesting
	public int numProcessingTimeTimers() {
		int count = 0;
		for (HeapInternalTimerService<?, ?> timerService : timerServices.values()) {
			count += timerService.numProcessingTimeTimers();
		}
		return count;
	}

	@VisibleForTesting
	public int numEventTimeTimers() {
		int count = 0;
		for (HeapInternalTimerService<?, ?> timerService : timerServices.values()) {
			count += timerService.numEventTimeTimers();
		}
		return count;
	}
}
