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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This is the transformation for the Broadcast State pattern. In a nutshell, this transformation
 * allows to take a broadcasted (non-keyed) stream, connect it with another keyed or non-keyed
 * stream, and apply a function on the resulting connected stream. This function will have access
 * to all the elements that belong to the non-keyed, broadcasted side, as this is kept in Flink's
 * state.
 *
 * <p>For more information see the
 * <a href="https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/state/broadcast_state.html">
 *     Broadcast State Pattern documentation page</a>.
 *
 * @param <IN1> The type of the elements in the non-broadcasted input.
 * @param <IN2> The type of the elements in the broadcasted input.
 * @param <OUT> The type of the elements that result from this transformation.
 */
@Internal
public class BroadcastStateTransformation<IN1, IN2, OUT> extends PhysicalTransformation<OUT> {

	private final Transformation<IN1> nonBroadcastStream;

	private final Transformation<IN2> broadcastStream;

	private final StreamOperatorFactory<OUT> operatorFactory;

	private final TypeInformation<?> stateKeyType;

	private final KeySelector<IN1, ?> keySelector;

	private BroadcastStateTransformation(
			final String name,
			final Transformation<IN1> inputStream,
			final Transformation<IN2> broadcastStream,
			final StreamOperatorFactory<OUT> operatorFactory,
			@Nullable final TypeInformation<?> keyType,
			@Nullable final KeySelector<IN1, ?> keySelector,
			final TypeInformation<OUT> outTypeInfo,
			final int parallelism) {
		super(name, outTypeInfo, parallelism);
		this.nonBroadcastStream = checkNotNull(inputStream);
		this.broadcastStream = checkNotNull(broadcastStream);
		this.operatorFactory = checkNotNull(operatorFactory);

		this.stateKeyType = keyType;
		this.keySelector = keySelector;
		updateManagedMemoryStateBackendUseCase(keySelector != null);
	}

	public Transformation<IN2> getBroadcastStream() {
		return broadcastStream;
	}

	public Transformation<IN1> getNonBroadcastStream() {
		return nonBroadcastStream;
	}

	public StreamOperatorFactory<OUT> getOperatorFactory() {
		return operatorFactory;
	}

	public TypeInformation<?> getStateKeyType() {
		return stateKeyType;
	}

	public KeySelector<IN1, ?> getKeySelector() {
		return keySelector;
	}

	@Override
	public void setChainingStrategy(ChainingStrategy strategy) {
		this.operatorFactory.getChainingStrategy();
	}

	@Override
	public List<Transformation<?>> getTransitivePredecessors() {
		final List<Transformation<?>> predecessors = new ArrayList<>();
		predecessors.add(this);
		predecessors.add(nonBroadcastStream);
		predecessors.add(broadcastStream);
		return predecessors;
	}

	@Override
	public List<Transformation<?>> getInputs() {
		final List<Transformation<?>> predecessors = new ArrayList<>();
		predecessors.add(nonBroadcastStream);
		predecessors.add(broadcastStream);
		return predecessors;
	}

	// ------------------------------- Static Constructors -------------------------------

	public static <IN1, IN2, OUT> BroadcastStateTransformation<IN1, IN2, OUT> forNonKeyedStream(
			final String name,
			final DataStream<IN1> nonBroadcastStream,
			final BroadcastStream<IN2> broadcastStream,
			final StreamOperatorFactory<OUT> operatorFactory,
			final TypeInformation<OUT> outTypeInfo,
			final int parallelism) {
		return new BroadcastStateTransformation<>(
				name,
				checkNotNull(nonBroadcastStream).getTransformation(),
				checkNotNull(broadcastStream).getTransformation(),
				operatorFactory,
				null,
				null,
				outTypeInfo,
				parallelism);
	}

	public static <IN1, IN2, OUT> BroadcastStateTransformation<IN1, IN2, OUT> forKeyedStream(
			final String name,
			final KeyedStream<IN1, ?> nonBroadcastStream,
			final BroadcastStream<IN2> broadcastStream,
			final StreamOperatorFactory<OUT> operatorFactory,
			final TypeInformation<OUT> outTypeInfo,
			final int parallelism) {
		return new BroadcastStateTransformation<>(
				name,
				checkNotNull(nonBroadcastStream).getTransformation(),
				checkNotNull(broadcastStream).getTransformation(),
				operatorFactory,
				nonBroadcastStream.getKeyType(),
				nonBroadcastStream.getKeySelector(),
				outTypeInfo,
				parallelism);
	}
}
