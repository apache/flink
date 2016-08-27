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
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class TestableKinesisDataFetcher extends KinesisDataFetcher<String> {

	private static final Object fakeCheckpointLock = new Object();

	private long numElementsCollected;

	public TestableKinesisDataFetcher(List<String> fakeStreams,
									  Properties fakeConfiguration,
									  int fakeTotalCountOfSubtasks,
									  int fakeTndexOfThisSubtask,
									  AtomicReference<Throwable> thrownErrorUnderTest,
									  LinkedList<KinesisStreamShardState> subscribedShardsStateUnderTest,
									  HashMap<String, String> subscribedStreamsToLastDiscoveredShardIdsStateUnderTest,
									  KinesisProxyInterface fakeKinesis) {
		super(fakeStreams,
			getMockedSourceContext(),
			fakeCheckpointLock,
			getMockedRuntimeContext(fakeTotalCountOfSubtasks, fakeTndexOfThisSubtask),
			fakeConfiguration,
			new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
			thrownErrorUnderTest,
			subscribedShardsStateUnderTest,
			subscribedStreamsToLastDiscoveredShardIdsStateUnderTest,
			fakeKinesis);

		this.numElementsCollected = 0;
	}

	public long getNumOfElementsCollected() {
		return numElementsCollected;
	}

	@Override
	protected KinesisDeserializationSchema<String> getClonedDeserializationSchema() {
		return new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema());
	}

	@Override
	protected void emitRecordAndUpdateState(String record, long recordTimestamp, int shardStateIndex, SequenceNumber lastSequenceNumber) {
		synchronized (fakeCheckpointLock) {
			this.numElementsCollected++;
			updateState(shardStateIndex, lastSequenceNumber);
		}
	}

	@SuppressWarnings("unchecked")
	private static SourceFunction.SourceContext<String> getMockedSourceContext() {
		return Mockito.mock(SourceFunction.SourceContext.class);
	}

	private static RuntimeContext getMockedRuntimeContext(final int fakeTotalCountOfSubtasks, final int fakeTndexOfThisSubtask) {
		RuntimeContext mockedRuntimeContext = Mockito.mock(RuntimeContext.class);

		Mockito.when(mockedRuntimeContext.getNumberOfParallelSubtasks()).thenAnswer(new Answer<Integer>() {
			@Override
			public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
				return fakeTotalCountOfSubtasks;
			}
		});

		Mockito.when(mockedRuntimeContext.getIndexOfThisSubtask()).thenAnswer(new Answer<Integer>() {
			@Override
			public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
				return fakeTndexOfThisSubtask;
			}
		});

		Mockito.when(mockedRuntimeContext.getTaskName()).thenAnswer(new Answer<String>() {
			@Override
			public String answer(InvocationOnMock invocationOnMock) throws Throwable {
				return "Fake Task";
			}
		});

		Mockito.when(mockedRuntimeContext.getTaskNameWithSubtasks()).thenAnswer(new Answer<String>() {
			@Override
			public String answer(InvocationOnMock invocationOnMock) throws Throwable {
				return "Fake Task (" + fakeTndexOfThisSubtask + "/" + fakeTotalCountOfSubtasks + ")";
			}
		});

		return mockedRuntimeContext;
	}
}
