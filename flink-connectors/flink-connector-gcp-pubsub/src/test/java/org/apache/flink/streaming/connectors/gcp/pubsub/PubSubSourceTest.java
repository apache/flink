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

package org.apache.flink.streaming.connectors.gcp.pubsub;

import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriberFactory;

import com.google.api.gax.rpc.UnaryCallable;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * Test for {@link SourceFunction}.
 */
@RunWith(MockitoJUnitRunner.class)
public class PubSubSourceTest {
	private static final String FIRST_MESSAGE = "FirstMessage";
	private static final String SECOND_MESSAGE = "SecondMessage";

	@Mock
	private org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<String> sourceContext;
	@Mock
	private PubSubDeserializationSchema<String> deserializationSchema;
	@Mock
	private StreamingRuntimeContext streamingRuntimeContext;
	@Mock
	private OperatorStateStore operatorStateStore;
	@Mock
	private FunctionInitializationContext functionInitializationContext;
	@Mock
	private MetricGroup metricGroup;
	@Mock
	private PubSubSubscriberFactory pubSubSubscriberFactory;
	@Mock
	private Credentials credentials;
	@Mock
	private SubscriberStub subscriberStub;
	@Mock
	private UnaryCallable<AcknowledgeRequest, Empty> acknowledgeCallable;

	private PubSubSource<String> pubSubSource;

	@Before
	public void setup() throws Exception {
		when(pubSubSubscriberFactory.getSubscriber(any(), eq(credentials))).thenReturn(subscriberStub);
		when(functionInitializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
		when(streamingRuntimeContext.isCheckpointingEnabled()).thenReturn(true);
		when(streamingRuntimeContext.getMetricGroup()).thenReturn(metricGroup);
		when(streamingRuntimeContext.getNumberOfParallelSubtasks()).thenReturn(12);
		when(functionInitializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
		when(operatorStateStore.getSerializableListState(any())).thenReturn(null);

		pubSubSource = PubSubSource.newBuilder(deserializationSchema, "project", "subscriptionName")
									.withPubSubSubscriberFactory(pubSubSubscriberFactory)
									.withCredentials(credentials)
									.withMaxMessagesPerPull(100)
									.build();
		pubSubSource.setRuntimeContext(streamingRuntimeContext);
		pubSubSource.initializeState(functionInitializationContext);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testOpenWithoutCheckpointing() throws Exception {
		when(streamingRuntimeContext.isCheckpointingEnabled()).thenReturn(false);
		pubSubSource.open(null);
	}

	@Test
	public void testOpenWithCheckpointing() throws Exception {
		when(streamingRuntimeContext.isCheckpointingEnabled()).thenReturn(true);

		pubSubSource.open(null);

		verify(pubSubSubscriberFactory, times(1)).getSubscriber(any(), eq(credentials));
	}

	@Test
	public void testProcessMessage() throws Exception {
		when(deserializationSchema.isEndOfStream(any())).thenReturn(false).thenReturn(false);
		when(deserializationSchema.deserialize(pubSubMessage(FIRST_MESSAGE))).thenReturn(FIRST_MESSAGE);
		when(deserializationSchema.deserialize(pubSubMessage(SECOND_MESSAGE))).thenReturn(SECOND_MESSAGE);
		when(sourceContext.getCheckpointLock()).thenReturn("some object to lock on");

		pubSubSource.open(null);
		pubSubSource.processMessage(sourceContext, asList(receivedMessage("firstAckId", pubSubMessage(FIRST_MESSAGE)),
															receivedMessage("secondAckId", pubSubMessage(SECOND_MESSAGE))));

		//verify handling messages
		verify(sourceContext, times(1)).getCheckpointLock();
		verify(deserializationSchema, times(1)).isEndOfStream(FIRST_MESSAGE);
		verify(deserializationSchema, times(1)).deserialize(pubSubMessage(FIRST_MESSAGE));
		verify(sourceContext, times(1)).collect(FIRST_MESSAGE);

		verify(deserializationSchema, times(1)).isEndOfStream(SECOND_MESSAGE);
		verify(deserializationSchema, times(1)).deserialize(pubSubMessage(SECOND_MESSAGE));
		verify(sourceContext, times(1)).collect(SECOND_MESSAGE);
	}

	@Test
	public void testMessagesAcknowledged() throws Exception {
		when(subscriberStub.acknowledgeCallable()).thenReturn(acknowledgeCallable);
		List<String> input = asList("firstAckId", "secondAckId");

		pubSubSource.open(null);
		pubSubSource.acknowledgeSessionIDs(input);

		ArgumentCaptor<AcknowledgeRequest> captor = ArgumentCaptor.forClass(AcknowledgeRequest.class);
		verify(acknowledgeCallable, times(1)).call(captor.capture());

		AcknowledgeRequest actual = captor.getValue();
		assertThat(actual.getAckIdsList().size(), is(2));
		assertThat(actual.getAckIdsList().get(0), is("firstAckId"));
		assertThat(actual.getAckIdsList().get(1), is("secondAckId"));
	}

	@Test
	public void testNoMessagesAreAcknowledgedAfterSourceStopOrCancel() throws Exception {
		List<String> input = asList("firstAckId", "secondAckId");

		pubSubSource.open(null);
		pubSubSource.cancel();

		pubSubSource.acknowledgeSessionIDs(input);
		verifyZeroInteractions(subscriberStub);
	}

	@Test
	public void testDuplicateMessagesAreIgnoredWhenParallelismIsOne() throws Exception {
		when(streamingRuntimeContext.getNumberOfParallelSubtasks()).thenReturn(1);
		when(sourceContext.getCheckpointLock()).thenReturn("some object to lock on");

		pubSubSource.open(null);

		ReceivedMessage message = receivedMessage("ackId", pubSubMessage(FIRST_MESSAGE));

		//Process first message
		when(deserializationSchema.deserialize(pubSubMessage(FIRST_MESSAGE))).thenReturn(FIRST_MESSAGE);
		pubSubSource.processMessage(sourceContext, singletonList(message));
		verify(sourceContext, times(1)).getCheckpointLock();
		verify(sourceContext, times(1)).collect(FIRST_MESSAGE);

		//Ignore second message
		pubSubSource.processMessage(sourceContext, singletonList(message));
		verify(sourceContext, times(2)).getCheckpointLock();
		verifyNoMoreInteractions(sourceContext);
	}

	@Test
	public void testDuplicateMessagesAreNotIgnoredWhenParallelismIsHigherThanOne() throws Exception {
		when(streamingRuntimeContext.getNumberOfParallelSubtasks()).thenReturn(12);
		when(sourceContext.getCheckpointLock()).thenReturn("some object to lock on");

		pubSubSource.open(null);

		ReceivedMessage message = receivedMessage("ackId", pubSubMessage(FIRST_MESSAGE));

		//Process first message
		when(deserializationSchema.deserialize(pubSubMessage(FIRST_MESSAGE))).thenReturn(FIRST_MESSAGE);
		pubSubSource.processMessage(sourceContext, singletonList(message));
		verify(sourceContext, times(1)).getCheckpointLock();
		verify(sourceContext, times(1)).collect(FIRST_MESSAGE);

		//Process second message
		pubSubSource.processMessage(sourceContext, singletonList(message));
		verify(sourceContext, times(2)).getCheckpointLock();
		verify(sourceContext, times(2)).collect(FIRST_MESSAGE);
		verifyNoMoreInteractions(sourceContext);
	}

	@Test
	public void testTypeInformationFromDeserializationSchema() {
		TypeInformation<String> schemaTypeInformation = TypeInformation.of(String.class);
		when(deserializationSchema.getProducedType()).thenReturn(schemaTypeInformation);

		TypeInformation<String> actualTypeInformation = pubSubSource.getProducedType();

		assertThat(actualTypeInformation, is(schemaTypeInformation));
		verify(deserializationSchema, times(1)).getProducedType();
	}

	@Test
	public void testStoppingConnectorWhenDeserializationSchemaIndicatesEndOfStream() throws Exception {
		when(deserializationSchema.deserialize(pubSubMessage(FIRST_MESSAGE))).thenReturn(FIRST_MESSAGE);
		when(sourceContext.getCheckpointLock()).thenReturn("some object to lock on");
		pubSubSource.open(null);

		when(deserializationSchema.isEndOfStream(FIRST_MESSAGE)).thenReturn(true);

		ReceivedMessage message = receivedMessage("ackId", pubSubMessage(FIRST_MESSAGE));

		//Process message
		pubSubSource.processMessage(sourceContext, singletonList(message));
		verify(deserializationSchema, times(1)).isEndOfStream(FIRST_MESSAGE);
		verify(sourceContext, times(1)).getCheckpointLock();
		verifyNoMoreInteractions(sourceContext);
	}

	private ReceivedMessage receivedMessage(String ackId, PubsubMessage pubsubMessage) {
		return ReceivedMessage.newBuilder()
								.setAckId(ackId)
								.setMessage(pubsubMessage)
								.build();
	}

	private PubsubMessage pubSubMessage(String message) {
		return PubsubMessage.newBuilder()
			.setMessageId("some id")
			.setData(ByteString.copyFrom(message.getBytes()))
			.build();
	}
}
