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

package org.apache.flink.streaming.connectors.pubsub;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * Test for {@link SourceFunction}.
 */
@RunWith(MockitoJUnitRunner.class)
public class PubSubSourceTest {
	private static final String MESSAGE = "Message";
	private static final byte[] SERIALIZED_MESSAGE = MESSAGE.getBytes();
	@Mock
	private SubscriberWrapper subscriberWrapper;
	@Mock
	private org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<String> sourceContext;
	@Mock
	private DeserializationSchema<String> deserializationSchema;
	@Mock
	private AckReplyConsumer ackReplyConsumer;
	@Mock
	private StreamingRuntimeContext streamingRuntimeContext;
	@Mock
	private RuntimeContext runtimeContext;
	@Mock
	private OperatorStateStore operatorStateStore;
	@Mock
	private FunctionInitializationContext functionInitializationContext;

	@Test
	public void testOpenWithCheckpointing() throws Exception {
		when(streamingRuntimeContext.isCheckpointingEnabled()).thenReturn(true);

		PubSubSource<String> pubSubSource = createTestSource();
		pubSubSource.setRuntimeContext(streamingRuntimeContext);
		pubSubSource.open(null);

		verify(subscriberWrapper, times(1)).initialize(pubSubSource);
	}

	@Test
	public void testRun() throws IOException {
		PubSubSource<String> pubSubSource = createTestSource();
		pubSubSource.run(sourceContext);

		verify(subscriberWrapper, times(1)).startBlocking();
	}

	@Test
	public void testWithCheckpoints() throws Exception {
		when(deserializationSchema.deserialize(SERIALIZED_MESSAGE)).thenReturn(MESSAGE);
		when(streamingRuntimeContext.isCheckpointingEnabled()).thenReturn(true);
		when(sourceContext.getCheckpointLock()).thenReturn("some object to lock on");
		when(functionInitializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
		when(operatorStateStore.getSerializableListState(any(String.class))).thenReturn(null);

		PubSubSource<String> pubSubSource = createTestSource();
		pubSubSource.initializeState(functionInitializationContext);
		pubSubSource.setRuntimeContext(streamingRuntimeContext);
		pubSubSource.open(null);
		verify(subscriberWrapper, times(1)).initialize(pubSubSource);

		pubSubSource.run(sourceContext);

		pubSubSource.receiveMessage(pubSubMessage(), ackReplyConsumer);

		verify(sourceContext, times(1)).getCheckpointLock();
		verify(sourceContext, times(1)).collect(MESSAGE);
		verifyZeroInteractions(ackReplyConsumer);
	}

	@Test
	public void testMessagesAcknowledged() throws Exception {
		when(streamingRuntimeContext.isCheckpointingEnabled()).thenReturn(true);

		PubSubSource<String> pubSubSource = createTestSource();
		pubSubSource.setRuntimeContext(streamingRuntimeContext);
		pubSubSource.open(null);

		List<AckReplyConsumer> input = Collections.singletonList(ackReplyConsumer);

		pubSubSource.acknowledgeSessionIDs(input);

		verify(ackReplyConsumer, times(1)).ack();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testOnceWithoutCheckpointing() throws Exception {
		PubSubSource<String> pubSubSource = createTestSource();
		pubSubSource.setRuntimeContext(runtimeContext);

		pubSubSource.open(null);
	}

	private PubSubSource<String> createTestSource() throws IOException {
		return PubSubSource.<String>newBuilder()
			.withoutCredentials()
			.withSubscriberWrapper(subscriberWrapper)
			.withDeserializationSchema(deserializationSchema)
			.build();
	}

	private PubsubMessage pubSubMessage() {
		return PubsubMessage.newBuilder()
			.setData(ByteString.copyFrom(SERIALIZED_MESSAGE))
			.build();
	}
}
