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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

/**
 * Extension of {@link OneInputStreamOperatorTestHarness} that allows the operator to get
 * a {@link KeyedStateBackend}.
 */
public class KeyedOneInputStreamOperatorTestHarness<K, IN, OUT>
		extends OneInputStreamOperatorTestHarness<IN, OUT> {

	public KeyedOneInputStreamOperatorTestHarness(
			OneInputStreamOperator<IN, OUT> operator,
			final KeySelector<IN, K> keySelector,
			TypeInformation<K> keyType,
			int maxParallelism,
			int numSubtasks,
			int subtaskIndex) throws Exception {
		this(SimpleOperatorFactory.of(operator), keySelector, keyType, maxParallelism, numSubtasks, subtaskIndex);
	}

	public KeyedOneInputStreamOperatorTestHarness(
			StreamOperatorFactory<OUT> operatorFactory,
			final KeySelector<IN, K> keySelector,
			TypeInformation<K> keyType,
			int maxParallelism,
			int numSubtasks,
			int subtaskIndex) throws Exception {
		super(operatorFactory, maxParallelism, numSubtasks, subtaskIndex);

		ClosureCleaner.clean(keySelector, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
		config.setStatePartitioner(0, keySelector);
		config.setStateKeySerializer(keyType.createSerializer(executionConfig));
	}

	public KeyedOneInputStreamOperatorTestHarness(
		StreamOperatorFactory<OUT> operatorFactory,
		final KeySelector<IN, K> keySelector,
		TypeInformation<K> keyType) throws Exception {
		this(operatorFactory, keySelector, keyType, 1, 1, 0);
	}

	public KeyedOneInputStreamOperatorTestHarness(
			OneInputStreamOperator<IN, OUT> operator,
			final KeySelector<IN, K> keySelector,
			TypeInformation<K> keyType) throws Exception {
		this(operator, keySelector, keyType, 1, 1, 0);
	}

	public KeyedOneInputStreamOperatorTestHarness(
			final OneInputStreamOperator<IN, OUT> operator,
			final KeySelector<IN, K> keySelector,
			final TypeInformation<K> keyType,
			final MockEnvironment environment) throws Exception {
		super(operator, environment);

		ClosureCleaner.clean(keySelector, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
		config.setStatePartitioner(0, keySelector);
		config.setStateKeySerializer(keyType.createSerializer(executionConfig));
	}

	public int numKeyedStateEntries() {
		AbstractStreamOperator<?> abstractStreamOperator = (AbstractStreamOperator<?>) operator;
		KeyedStateBackend<Object> keyedStateBackend = abstractStreamOperator.getKeyedStateBackend();
		if (keyedStateBackend instanceof HeapKeyedStateBackend) {
			return ((HeapKeyedStateBackend) keyedStateBackend).numKeyValueStateEntries();
		} else {
			throw new UnsupportedOperationException();
		}
	}

	public <N> int numKeyedStateEntries(N namespace) {
		AbstractStreamOperator<?> abstractStreamOperator = (AbstractStreamOperator<?>) operator;
		KeyedStateBackend<Object> keyedStateBackend = abstractStreamOperator.getKeyedStateBackend();
		if (keyedStateBackend instanceof HeapKeyedStateBackend) {
			return ((HeapKeyedStateBackend) keyedStateBackend).numKeyValueStateEntries(namespace);
		} else {
			throw new UnsupportedOperationException();
		}
	}
}
