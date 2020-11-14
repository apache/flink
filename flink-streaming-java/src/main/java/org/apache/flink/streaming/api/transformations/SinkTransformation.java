/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link Transformation} for {@link Sink}.
 *
 * @param <InputT> The input type of the {@link SinkWriter}
 * @param <CommT> The committable type of the {@link SinkWriter}
 * @param <WriterStateT> The state type of the {@link SinkWriter}
 * @param <GlobalCommT> The global committable type of the {@link org.apache.flink.api.connector.sink.GlobalCommitter}
 */
@Internal
public class SinkTransformation<InputT, CommT, WriterStateT, GlobalCommT> extends PhysicalTransformation<Object> {

	private final Transformation<InputT> input;

	private final Sink<InputT, CommT, WriterStateT, GlobalCommT> sink;

	private ChainingStrategy chainingStrategy;

	public SinkTransformation(
			Transformation<InputT> input,
			Sink<InputT, CommT, WriterStateT, GlobalCommT> sink,
			String name,
			int parallelism) {
		super(name, TypeExtractor.getForClass(Object.class), parallelism);
		this.input = checkNotNull(input);
		this.sink = checkNotNull(sink);
	}

	@Override
	public void setChainingStrategy(ChainingStrategy strategy) {
		chainingStrategy = checkNotNull(strategy);
	}

	@Override
	public List<Transformation<?>> getTransitivePredecessors() {
		final List<Transformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}

	@Override
	public List<Transformation<?>> getInputs() {
		return Collections.singletonList(input);
	}

	@Override
	public void setUidHash(String uidHash) {
		throw new UnsupportedOperationException("Setting a UidHash is not supported for SinkTransformation.");
	}

	@Override
	public void setResources(ResourceSpec minResources, ResourceSpec preferredResources) {
		throw new UnsupportedOperationException(
				"Do not support set resources for SinkTransformation.");
	}

	@Override
	public Optional<Integer> declareManagedMemoryUseCaseAtOperatorScope(
			ManagedMemoryUseCase managedMemoryUseCase,
			int weight) {
		throw new UnsupportedOperationException(
				"Declaring managed memory use cases is not supported for SinkTransformation.");
	}

	@Override
	public void declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase managedMemoryUseCase) {
		throw new UnsupportedOperationException(
				"Declaring managed memory use cases is not supported for SinkTransformation.");
	}

	public ChainingStrategy getChainingStrategy() {
		return chainingStrategy;
	}

	public Sink<InputT, CommT, WriterStateT, GlobalCommT> getSink() {
		return sink;
	}
}
