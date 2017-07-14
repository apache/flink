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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;

/**
 * Extension of {@link TwoInputStreamOperatorTestHarness} that allows the operator to get
 * a {@link KeyedStateBackend}.
 */
public class KeyedTwoInputStreamOperatorTestHarness<K, IN1, IN2, OUT>
		extends TwoInputStreamOperatorTestHarness<IN1, IN2, OUT> {

	// in case the operator creates one we store it here so that we
	// can snapshot its state
	private AbstractKeyedStateBackend<?> keyedStateBackend = null;

	// when we restore we keep the state here so that we can call restore
	// when the operator requests the keyed state backend
	private Collection<KeyedStateHandle> restoredKeyedState = null;

	public KeyedTwoInputStreamOperatorTestHarness(
			TwoInputStreamOperator<IN1, IN2, OUT> operator,
			KeySelector<IN1, K> keySelector1,
			KeySelector<IN2, K> keySelector2,
			TypeInformation<K> keyType,
			int maxParallelism,
			int numSubtasks,
			int subtaskIndex) throws Exception {
		super(operator, maxParallelism, numSubtasks, subtaskIndex);

		ClosureCleaner.clean(keySelector1, false);
		ClosureCleaner.clean(keySelector2, false);
		config.setStatePartitioner(0, keySelector1);
		config.setStatePartitioner(1, keySelector2);
		config.setStateKeySerializer(keyType.createSerializer(executionConfig));

		setupMockTaskCreateKeyedBackend();
	}

	public KeyedTwoInputStreamOperatorTestHarness(
			TwoInputStreamOperator<IN1, IN2, OUT> operator,
			final KeySelector<IN1, K> keySelector1,
			final KeySelector<IN2, K> keySelector2,
			TypeInformation<K> keyType) throws Exception {
		this(operator, keySelector1, keySelector2, keyType, 1, 1, 0);
	}

	private void setupMockTaskCreateKeyedBackend() {

		try {
			doAnswer(new Answer<KeyedStateBackend>() {
				@Override
				public KeyedStateBackend answer(InvocationOnMock invocationOnMock) throws Throwable {

					final TypeSerializer keySerializer = (TypeSerializer) invocationOnMock.getArguments()[0];
					final int numberOfKeyGroups = (Integer) invocationOnMock.getArguments()[1];
					final KeyGroupRange keyGroupRange = (KeyGroupRange) invocationOnMock.getArguments()[2];

					if (keyedStateBackend != null) {
						keyedStateBackend.close();
					}

					keyedStateBackend = stateBackend.createKeyedStateBackend(
							mockTask.getEnvironment(),
							new JobID(),
							"test_op",
							keySerializer,
							numberOfKeyGroups,
							keyGroupRange,
							mockTask.getEnvironment().getTaskKvStateRegistry());
					if (restoredKeyedState != null) {
						keyedStateBackend.restore(restoredKeyedState);
					}
					return keyedStateBackend;
				}
			}).when(mockTask).createKeyedStateBackend(any(TypeSerializer.class), anyInt(), any(KeyGroupRange.class));
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	@Override
	public void initializeState(OperatorStateHandles operatorStateHandles) throws Exception {
		if (restoredKeyedState != null) {
			restoredKeyedState = operatorStateHandles.getManagedKeyedState();
		}

		super.initializeState(operatorStateHandles);
	}

	public int numKeyedStateEntries() {
		if (keyedStateBackend instanceof HeapKeyedStateBackend) {
			return ((HeapKeyedStateBackend) keyedStateBackend).numStateEntries();
		} else {
			throw new UnsupportedOperationException();
		}
	}
}
