/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockContext<IN, OUT> {
	
	private List<OUT> outputs;

	private MockOutput<OUT> output;

	public MockContext(Collection<IN> inputs) {
		if (inputs.isEmpty()) {
			throw new RuntimeException("Inputs must not be empty");
		}

		outputs = new ArrayList<OUT>();
		output = new MockOutput<OUT>(outputs);
	}

	public List<OUT> getOutputs() {
		return outputs;
	}

	public Output<StreamRecord<OUT>> getOutput() {
		return output;
	}

	public static <IN, OUT> List<OUT> createAndExecute(OneInputStreamOperator<IN, OUT> operator, List<IN> inputs) throws Exception {
		return createAndExecuteForKeyedStream(operator, inputs, null, null);
	}
	
	public static <IN, OUT, KEY> List<OUT> createAndExecuteForKeyedStream(
				OneInputStreamOperator<IN, OUT> operator, List<IN> inputs,
				KeySelector<IN, KEY> keySelector, TypeInformation<KEY> keyType) throws Exception {

		OneInputStreamOperatorTestHarness<IN, OUT> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, keyType);

		testHarness.setup();
		testHarness.open();
		
		operator.open();

		for (IN in: inputs) {
			testHarness.processElement(new StreamRecord<>(in));
		}

		testHarness.close();

		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

		List<OUT> result = new ArrayList<>();

		for (Object o : output) {
			if (o instanceof StreamRecord) {
				result.add((OUT) ((StreamRecord) o).getValue());
			}
		}

		return result;
	}

	private static StreamTask<?, ?> createMockTaskWithTimer(
			final ScheduledExecutorService timerService, final Object lock)
	{
		StreamTask<?, ?> task = mock(StreamTask.class);
		when(task.getAccumulatorMap()).thenReturn(new HashMap<String, Accumulator<?, ?>>());
		when(task.getName()).thenReturn("Test task name");
		when(task.getExecutionConfig()).thenReturn(new ExecutionConfig());
		when(task.getEnvironment()).thenReturn(new MockEnvironment("MockTask", 3 * 1024 * 1024, new MockInputSplitProvider(), 1024));
		when(task.getCheckpointLock()).thenReturn(lock);

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				final Long timestamp = (Long) invocationOnMock.getArguments()[0];
				final Triggerable target = (Triggerable) invocationOnMock.getArguments()[1];
				timerService.schedule(
						new Callable<Object>() {
							@Override
							public Object call() throws Exception {
								synchronized (lock) {
									target.trigger(timestamp);
								}
								return null;
							}
						},
						timestamp - System.currentTimeMillis(),
						TimeUnit.MILLISECONDS);
				return null;
			}
		}).when(task).registerTimer(anyLong(), any(Triggerable.class));

		return task;
	}
}
