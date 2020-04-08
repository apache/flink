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

package org.apache.flink.streaming.util;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

/**
 * The abstract basic stream operator test harness builder.
 *
 * @param <T> The self-type of builder.
 * @param <OUT> The output type of the operator.
 */
public abstract class AbstractBasicStreamOperatorTestHarnessBuilder<T extends AbstractBasicStreamOperatorTestHarnessBuilder<T, OUT>, OUT>
	implements StreamOperatorTestHarnessBuilder<AbstractStreamOperatorTestHarness<OUT>, OUT> {

	protected StreamOperator<OUT> operator;
	protected OperatorID operatorID = new OperatorID();

	protected StreamOperatorFactory<OUT> factory;

	protected MockEnvironment environment;

	// use this as default for tests
	protected int maxParallelism = 1;
	protected int parallelism = 1;
	protected int subtaskIndex = 0;

	protected boolean isInternalEnvironment = false;

	@SuppressWarnings("unchecked")
	public T setStreamOperator(StreamOperator<OUT> operator) {
		this.operator = operator;
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	public T setStreamOperatorFactory(StreamOperatorFactory<OUT> factory) {
		this.factory = factory;
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	public T setMaxParallelism(int maxParallelism) {
		this.maxParallelism = maxParallelism;
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	public T setParallelism(int parallelism) {
		this.parallelism = parallelism;
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	public T setSubtaskIndex(int subtaskIndex) {
		this.subtaskIndex = subtaskIndex;
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	public T setOperatorID(OperatorID operatorID) {
		this.operatorID = operatorID;
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	public T setEnvironment(MockEnvironment environment) {
		this.environment = environment;
		return (T) this;
	}

	protected MockEnvironment computeEnvironmentIfAbsent() {
		if (environment == null) {
			this.environment = AbstractStreamOperatorTestHarness.buildMockEnvironment(maxParallelism, parallelism, subtaskIndex);
			this.isInternalEnvironment = true;
		}
		return environment;
	}

	protected StreamOperatorFactory<OUT> computeFactoryIfAbsent() {
		if (factory == null) {
			this.factory = AbstractStreamOperatorTestHarness.buildOperatorFactory(operator);
		}
		return factory;
	}
}
