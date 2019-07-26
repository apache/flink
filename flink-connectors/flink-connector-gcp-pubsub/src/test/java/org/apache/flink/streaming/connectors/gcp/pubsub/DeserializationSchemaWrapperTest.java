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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DeserializationSchema}.
 */
@RunWith(MockitoJUnitRunner.class)
public class DeserializationSchemaWrapperTest {
	@Mock
	private DeserializationSchema<String> deserializationSchema;

	@InjectMocks
	private DeserializationSchemaWrapper<String> deserializationSchemaWrapper;

	@Test
	public void testProducedType() {
		TypeInformation<String> typeInformation = TypeInformation.of(String.class);
		when(deserializationSchema.getProducedType()).thenReturn(typeInformation);

		assertThat(deserializationSchemaWrapper.getProducedType(), is(typeInformation));
		verify(deserializationSchema, times(1)).getProducedType();
	}

	@Test
	public void testEndOfStream() {
		String input = "some-input";
		when(deserializationSchema.isEndOfStream(any())).thenReturn(true);

		assertThat(deserializationSchemaWrapper.isEndOfStream(input), is(true));
		verify(deserializationSchema, times(1)).isEndOfStream(input);
	}

	@Test
	public void testDeserialize() throws Exception {
		String inputAsString = "some-input";
		byte[] inputAsBytes = inputAsString.getBytes();

		when(deserializationSchema.deserialize(any())).thenReturn(inputAsString);

		assertThat(deserializationSchemaWrapper.deserialize(pubSubMessage(inputAsString)), is(inputAsString));
		verify(deserializationSchema, times(1)).deserialize(inputAsBytes);
	}

	private PubsubMessage pubSubMessage(String message) {
		return PubsubMessage.newBuilder()
			.setMessageId("some id")
			.setData(ByteString.copyFrom(message.getBytes()))
			.build();
	}
}
