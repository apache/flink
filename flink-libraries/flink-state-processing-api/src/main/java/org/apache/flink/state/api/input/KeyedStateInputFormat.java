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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.api.input.splits.KeyGroupRangeInputSplit;
import org.apache.flink.state.api.runtime.NeverFireProcessingTimeService;
import org.apache.flink.state.api.runtime.SavepointEnvironment;
import org.apache.flink.state.api.runtime.SavepointRuntimeContext;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.StreamOperatorStateContext;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Input format for reading partitioned state.
 *
 * @param <K> The type of the key.
 * @param <OUT> The type of the output of the {@link KeyedStateReaderFunction}.
 */
@Internal
public class KeyedStateInputFormat<K, OUT> extends SavepointInputFormat<OUT, KeyGroupRangeInputSplit> implements KeyContext {

	private static final String USER_TIMERS_NAME = "user-timers";

	private final StateBackend stateBackend;

	private final TypeInformation<K> keyType;

	private final KeyedStateReaderFunction<K, OUT> userFunction;

	private transient TypeSerializer<K> keySerializer;

	private transient CloseableRegistry registry;

	private transient BufferingCollector<OUT> out;

	private transient Iterator<K> keys;

	private transient AbstractKeyedStateBackend<K> keyedStateBackend;

	private transient Context ctx;

	/**
	 * Creates an input format for reading partitioned state from an operator in a savepoint.
	 *
	 * @param savepointPath The path to an existing savepoint.
	 * @param uid           The uid of an operator.
	 * @param stateBackend  The state backed used to snapshot the operator.
	 * @param keyType       The type information describing the key type.
	 * @param userFunction  The {@link KeyedStateReaderFunction} called for each key in the operator.
	 */
	public KeyedStateInputFormat(
		String savepointPath,
		String uid,
		StateBackend stateBackend,
		TypeInformation<K> keyType,
		KeyedStateReaderFunction<K, OUT> userFunction) {
		super(savepointPath, uid);
		this.stateBackend = stateBackend;
		this.keyType = keyType;
		this.userFunction = userFunction;
	}

	@Override
	public KeyGroupRangeInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		final OperatorState operatorState = getOperatorState();

		return getKeyGroupRangeInputSplits(minNumSplits, operatorState);
	}

	@Override
	public void openInputFormat() {
		out = new BufferingCollector<>();
		keySerializer = keyType.createSerializer(getRuntimeContext().getExecutionConfig());
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open(KeyGroupRangeInputSplit split) throws IOException {
		registry = new CloseableRegistry();

		final Environment environment = new SavepointEnvironment
			.Builder(getRuntimeContext(), split.getNumKeyGroups())
			.setSubtaskIndex(split.getSplitNumber())
			.setPrioritizedOperatorSubtaskState(split.getPrioritizedOperatorSubtaskState())
			.build();

		final StreamOperatorStateContext context = getStreamOperatorStateContext(environment);

		keyedStateBackend = (AbstractKeyedStateBackend<K>) context.keyedStateBackend();
		keys = getKeyIterator();
		ctx = new Context();
	}

	@SuppressWarnings("unchecked")
	private Iterator<K> getKeyIterator() throws IOException {
		Preconditions.checkNotNull(keyedStateBackend);
		final DefaultKeyedStateStore keyedStateStore = new DefaultKeyedStateStore(keyedStateBackend, getRuntimeContext().getExecutionConfig());

		final List<StateDescriptor<?, ?>> stateDescriptors;
		try (SavepointRuntimeContext ctx = new SavepointRuntimeContext(getRuntimeContext(), keyedStateStore)) {
			FunctionUtils.setFunctionRuntimeContext(userFunction, ctx);
			FunctionUtils.openFunction(userFunction, new Configuration());
			stateDescriptors = ctx.getStateDescriptors();
		} catch (Exception e) {
			throw new IOException("Failed to open user defined function", e);
		}

		return new MultiStateKeyIterator<>(stateDescriptors, keyedStateBackend);
	}

	private StreamOperatorStateContext getStreamOperatorStateContext(Environment environment) throws IOException {
		StreamTaskStateInitializer initializer = new StreamTaskStateInitializerImpl(
			environment,
			stateBackend,
			new NeverFireProcessingTimeService());

		try {
			return initializer.streamOperatorStateContext(
				operatorID,
				uid,
				this,
				keySerializer,
				registry,
				getRuntimeContext().getMetricGroup());
		} catch (Exception e) {
			throw new IOException("Failed to restore state backend", e);
		}
	}

	@Override
	public void close() throws IOException {
		registry.close();
	}

	@Override
	public boolean reachedEnd() {
		return !out.hasNext() && !keys.hasNext();
	}

	@Override
	public OUT nextRecord(OUT reuse) throws IOException {
		if (out.hasNext()) {
			return out.next();
		}

		final K key = keys.next();
		setCurrentKey(key);

		try {
			userFunction.readKey(key, ctx, out);
		} catch (Exception e) {
			throw new IOException("User defined function KeyedStateReaderFunction#readKey threw an exception", e);
		}

		keys.remove();

		return out.next();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void setCurrentKey(Object key) {
		keyedStateBackend.setCurrentKey((K) key);
	}

	@Override
	public Object getCurrentKey() {
		return keyedStateBackend.getCurrentKey();
	}

	@VisibleForTesting
	static KeyGroupRangeInputSplit[] getKeyGroupRangeInputSplits(int minNumSplits, OperatorState operatorState) {
		final int maxParallelism = operatorState.getMaxParallelism();

		final List<KeyGroupRange> keyGroups = sortedKeyGroupRanges(minNumSplits, maxParallelism);

		return CollectionUtil.mapWithIndex(
			keyGroups,
			(keyGroupRange, index) -> createKeyGroupRangeInputSplit(
				operatorState,
				maxParallelism,
				keyGroupRange,
				index)
		).toArray(KeyGroupRangeInputSplit[]::new);
	}

	private static KeyGroupRangeInputSplit createKeyGroupRangeInputSplit(
		OperatorState operatorState,
		int maxParallelism,
		KeyGroupRange keyGroupRange,
		Integer index) {

		final List<KeyedStateHandle> managedKeyedState = StateAssignmentOperation.getManagedKeyedStateHandles(operatorState, keyGroupRange);
		final List<KeyedStateHandle> rawKeyedState = StateAssignmentOperation.getRawKeyedStateHandles(operatorState, keyGroupRange);

		return new KeyGroupRangeInputSplit(managedKeyedState, rawKeyedState, maxParallelism, index);
	}

	@Nonnull
	private static List<KeyGroupRange> sortedKeyGroupRanges(int minNumSplits, int maxParallelism) {
		List<KeyGroupRange> keyGroups = StateAssignmentOperation.createKeyGroupPartitions(
			maxParallelism,
			Math.min(minNumSplits, maxParallelism));

		keyGroups.sort(Comparator.comparing(KeyGroupRange::getStartKeyGroup));
		return keyGroups;
	}

	private static class Context implements KeyedStateReaderFunction.Context {}
}
