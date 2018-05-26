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
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Test for {@link PubSubSource}.
 */
@RunWith(MockitoJUnitRunner.class)
public class PubSubSourceTest {
	private static final String MESSAGE = "Message";
	private static final byte[] SERIALIZED_MESSAGE = MESSAGE.getBytes();
	@Mock
	private SubscriberFactory subscriberFactory;
	@Mock
	private SourceFunction.SourceContext<String> sourceContext;
	@Mock
	private DeserializationSchema<String> deserializationSchema;
	@Mock
	private AckReplyConsumer ackReplyConsumer;

	@Test
	public void testOpen() throws Exception {
		PubSubSource<String> pubSubSource = new PubSubSource<>(subscriberFactory, deserializationSchema);
		pubSubSource.open(null);

		verify(subscriberFactory, times(1)).initialize(pubSubSource);
	}

	@Test
	public void testRun() {
		PubSubSource<String> pubSubSource = new PubSubSource<>(subscriberFactory, deserializationSchema);
		pubSubSource.run(sourceContext);

		verify(subscriberFactory, times(1)).startBlocking();
	}

	@Test
	public void testWithoutCheckpoints() throws Exception {
		when(deserializationSchema.deserialize(SERIALIZED_MESSAGE)).thenReturn(MESSAGE);

		PubSubSource<String> pubSubSource = new PubSubSource<>(subscriberFactory, deserializationSchema);
		pubSubSource.open(null);
		pubSubSource.run(sourceContext);

		pubSubSource.receiveMessage(pubSubMessage(), ackReplyConsumer);

		verify(sourceContext, times(1)).collect(MESSAGE);
		verify(ackReplyConsumer, times(1)).ack();
	}

	private PubsubMessage pubSubMessage() {
		return PubsubMessage.newBuilder()
							.setData(ByteString.copyFrom(SERIALIZED_MESSAGE))
							.build();
	}
}
