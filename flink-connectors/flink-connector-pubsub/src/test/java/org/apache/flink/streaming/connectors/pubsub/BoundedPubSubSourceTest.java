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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * Tests for {@link BoundedPubSubSource}.
 */
public class BoundedPubSubSourceTest {
	private final Bound<Object> bound = mock(Bound.class);
	private final SubscriberWrapper subscriberWrapper = mock(SubscriberWrapper.class);
	private final SourceFunction.SourceContext<Object> sourceContext = mock(SourceFunction.SourceContext.class);
	private final AckReplyConsumer ackReplyConsumer = mock(AckReplyConsumer.class);
	private final DeserializationSchema<Object> deserializationSchema = mock(DeserializationSchema.class);

	private FunctionInitializationContext functionInitializationContext = mock(FunctionInitializationContext.class);
	private OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
	private StreamingRuntimeContext streamingRuntimeContext = mock(StreamingRuntimeContext.class);

	@Test
	public void testBoundIsUsed() throws Exception {
		BoundedPubSubSource<Object> boundedPubSubSource = createAndInitializeBoundedPubSubSource();
		boundedPubSubSource.setBound(bound);

		boundedPubSubSource.run(sourceContext);
		verify(bound, times(1)).start(boundedPubSubSource);

		boundedPubSubSource.receiveMessage(pubSubMessage(), ackReplyConsumer);
		verify(bound, times(1)).receivedMessage();
	}

	private BoundedPubSubSource<Object> createAndInitializeBoundedPubSubSource() throws Exception {
		when(sourceContext.getCheckpointLock()).thenReturn(new Object());
		when(functionInitializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
		when(operatorStateStore.getSerializableListState(any(String.class))).thenReturn(null);
		when(streamingRuntimeContext.isCheckpointingEnabled()).thenReturn(true);

		BoundedPubSubSource<Object> boundedPubSubSource = BoundedPubSubSource.newBuilder()
			.withoutCredentials()
			.withSubscriberWrapper(subscriberWrapper)
			.withDeserializationSchema(deserializationSchema)
			.build();
		boundedPubSubSource.initializeState(functionInitializationContext);
		boundedPubSubSource.setRuntimeContext(streamingRuntimeContext);
		boundedPubSubSource.open(null);

		return boundedPubSubSource;
	}

	private PubsubMessage pubSubMessage() {
		return PubsubMessage.newBuilder()
			.setMessageId("message-id")
			.setData(ByteString.copyFrom("some-message".getBytes()))
			.build();
	}
}
