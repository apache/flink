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

package org.apache.flink.streaming.connectors.kinesis.testutils;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Properties;

/**
 * Extension of the {@link FlinkKinesisConsumer} for testing.
 */
public class TestableFlinkKinesisConsumer extends FlinkKinesisConsumer<String> {

	private final RuntimeContext mockedRuntimeCtx;

	public TestableFlinkKinesisConsumer(String fakeStream,
										Properties fakeConfiguration,
										final int totalNumOfConsumerSubtasks,
										final int indexOfThisConsumerSubtask) {
		super(fakeStream, new SimpleStringSchema(), fakeConfiguration);

		this.mockedRuntimeCtx = Mockito.mock(RuntimeContext.class);

		Mockito.when(mockedRuntimeCtx.getNumberOfParallelSubtasks()).thenAnswer(new Answer<Integer>() {
			@Override
			public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
				return totalNumOfConsumerSubtasks;
			}
		});

		Mockito.when(mockedRuntimeCtx.getIndexOfThisSubtask()).thenAnswer(new Answer<Integer>() {
			@Override
			public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
				return indexOfThisConsumerSubtask;
			}
		});
	}

	@Override
	public RuntimeContext getRuntimeContext() {
		return this.mockedRuntimeCtx;
	}
}
