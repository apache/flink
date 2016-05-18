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
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.mockito.Mockito;

import java.util.Properties;

/**
 * A testable FlinkKinesisConsumer that overrides getRuntimeContext to return a dummy StreamRuntimeContext.
 */
public class TestableFlinkKinesisConsumer extends FlinkKinesisConsumer {

	private final int fakeNumFlinkConsumerTasks;
	private final int fakeThisConsumerTaskIndex;
	private final String fakeThisConsumerTaskName;


	public TestableFlinkKinesisConsumer(String fakeStreamName,
										int fakeNumFlinkConsumerTasks,
										int fakeThisConsumerTaskIndex,
										String fakeThisConsumerTaskName,
										Properties configProps) {
		super(fakeStreamName, new SimpleStringSchema(), configProps);
		this.fakeNumFlinkConsumerTasks = fakeNumFlinkConsumerTasks;
		this.fakeThisConsumerTaskIndex = fakeThisConsumerTaskIndex;
		this.fakeThisConsumerTaskName = fakeThisConsumerTaskName;
	}

	@Override
	public RuntimeContext getRuntimeContext() {
		StreamingRuntimeContext runtimeContextMock = Mockito.mock(StreamingRuntimeContext.class);
		Mockito.when(runtimeContextMock.getNumberOfParallelSubtasks()).thenReturn(fakeNumFlinkConsumerTasks);
		Mockito.when(runtimeContextMock.getIndexOfThisSubtask()).thenReturn(fakeThisConsumerTaskIndex);
		Mockito.when(runtimeContextMock.getTaskName()).thenReturn(fakeThisConsumerTaskName);
		return runtimeContextMock;
	}

}
