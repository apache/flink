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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.AcknowledgeIdsForCheckpoint;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.AcknowledgeOnCheckpoint;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriberFactory;

import com.google.api.gax.rpc.UnaryCallable;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
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
	private SourceFunction.SourceContext<String> sourceContext;
	@Mock
	private PubSubDeserializationSchema<String> deserializationSchema;
	@Mock
	private PubSubSource.AcknowledgeOnCheckpointFactory acknowledgeOnCheckpointFactory;
	@Mock
	private AcknowledgeOnCheckpoint<String> acknowledgeOnCheckpoint;
	@Mock
	private StreamingRuntimeContext streamingRuntimeContext;
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
		when(streamingRuntimeContext.isCheckpointingEnabled()).thenReturn(true);
		when(streamingRuntimeContext.getMetricGroup()).thenReturn(metricGroup);
		when(acknowledgeOnCheckpointFactory.create(any())).thenReturn(acknowledgeOnCheckpoint);

		pubSubSource = new PubSubSource<>(deserializationSchema,
			pubSubSubscriberFactory,
			credentials,
			ProjectSubscriptionName.format("project", "subscriptionName"),
			100,
			1000,
			acknowledgeOnCheckpointFactory);
		pubSubSource.setRuntimeContext(streamingRuntimeContext);
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
		verify(acknowledgeOnCheckpointFactory, times(1)).create(pubSubSource);
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
		verify(acknowledgeOnCheckpoint, times(1)).addAcknowledgeId("firstAckId");

		verify(deserializationSchema, times(1)).isEndOfStream(SECOND_MESSAGE);
		verify(deserializationSchema, times(1)).deserialize(pubSubMessage(SECOND_MESSAGE));
		verify(sourceContext, times(1)).collect(SECOND_MESSAGE);
		verify(acknowledgeOnCheckpoint, times(1)).addAcknowledgeId("secondAckId");
	}

	@Test
	public void testMessagesAcknowledged() throws Exception {
		when(subscriberStub.acknowledgeCallable()).thenReturn(acknowledgeCallable);
		List<String> input = asList("firstAckId", "secondAckId");

		pubSubSource.open(null);
		pubSubSource.acknowledge(input);

		ArgumentCaptor<AcknowledgeRequest> captor = ArgumentCaptor.forClass(AcknowledgeRequest.class);
		verify(acknowledgeCallable, times(1)).call(captor.capture());

		AcknowledgeRequest actual = captor.getValue();
		assertThat(actual.getAckIdsList(), hasSize(2));
		assertThat(actual.getAckIdsList(), containsInAnyOrder("firstAckId", "secondAckId"));
	}

	@Test
	public void testMessagesAcknowledgedInBatches() throws Exception {
		when(subscriberStub.acknowledgeCallable()).thenReturn(acknowledgeCallable);
		List<String> input = IntStream.range(0, 50000)
			.mapToObj(i -> String.format("id-%d", i))
			.collect(toList());

		pubSubSource.open(null);
		pubSubSource.acknowledge(input);

		ArgumentCaptor<AcknowledgeRequest> captor = ArgumentCaptor.forClass(AcknowledgeRequest.class);
		verify(acknowledgeCallable, times(2)).call(captor.capture());

		List<AcknowledgeRequest> actual = captor.getAllValues();

		assertThat(actual, hasSize(2));
		assertThat(actual.get(0).getAckIdsList(), hasSize(47546));
		assertThat(actual.get(0).getAckIdsList(), containsInAnyOrder(input.subList(0, 47546).toArray()));
		assertThat(actual.get(1).getAckIdsList(), hasSize(2454));
		assertThat(actual.get(1).getAckIdsList(), containsInAnyOrder(input.subList(47546, 50000).toArray()));
	}

	@Test
	public void testNoMessagesAreAcknowledgedAfterSourceStopOrCancel() throws Exception {
		List<String> input = asList("firstAckId", "secondAckId");

		pubSubSource.open(null);
		pubSubSource.cancel();

		pubSubSource.acknowledge(input);
		verifyZeroInteractions(subscriberStub);
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

	@Test
	public void testNotifyCheckpointComplete() throws Exception {
		pubSubSource.open(null);
		pubSubSource.notifyCheckpointComplete(45L);

		verify(acknowledgeOnCheckpoint, times(1)).notifyCheckpointComplete(45L);
	}

	@Test
	public void testRestoreState() throws Exception {
		pubSubSource.open(null);

		List<AcknowledgeIdsForCheckpoint<String>> input = new ArrayList<>();
		pubSubSource.restoreState(input);

		verify(acknowledgeOnCheckpoint, times(1)).restoreState(refEq(input));
	}

	@Test
	public void testSnapshotState() throws Exception {
		pubSubSource.open(null);
		pubSubSource.snapshotState(1337L, 15000L);

		verify(acknowledgeOnCheckpoint, times(1)).snapshotState(1337L, 15000L);
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
