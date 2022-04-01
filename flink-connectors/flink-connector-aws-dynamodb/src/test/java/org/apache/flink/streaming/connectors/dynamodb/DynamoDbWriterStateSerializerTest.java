/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.dynamodb;

import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.apache.flink.streaming.connectors.dynamodb.sink.DynamoDbWriteRequest;
import org.apache.flink.streaming.connectors.dynamodb.sink.DynamoDbWriterStateSerializer;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for serializing and deserialzing a collection of {@link DynamoDbWriteRequest} with {@link
 * DynamoDbWriterStateSerializer}.
 */
public class DynamoDbWriterStateSerializerTest {

    private static final DynamoDbWriterStateSerializer SERIALIZER =
            new DynamoDbWriterStateSerializer();

    @Test
    public void testStateSerDe() throws IOException {
        List<DynamoDbWriteRequest> requests = new ArrayList<>();
        DynamoDbWriteRequest dynamoDbWriteRequest =
                new DynamoDbWriteRequest("Table", WriteRequest.builder().build());
        requests.add(dynamoDbWriteRequest);

        BufferedRequestState<DynamoDbWriteRequest> state =
                new BufferedRequestState<>(
                        Arrays.asList(
                                new RequestEntryWrapper(
                                        new DynamoDbWriteRequest(
                                                "Table", WriteRequest.builder().build()),
                                        1)));

        byte[] serialize = SERIALIZER.serialize(state);
        BufferedRequestState<DynamoDbWriteRequest> deserializedState =
                SERIALIZER.deserialize(1, serialize);
        Assertions.assertThat(state.getStateSize()).isEqualTo(deserializedState.getStateSize());

        for (int i = 0; i < state.getStateSize(); i++) {
            Assertions.assertThat((state.getBufferedRequestEntries().get(i).getSize()))
                    .isEqualTo(deserializedState.getBufferedRequestEntries().get(i).getSize());
            Assertions.assertThat((state.getBufferedRequestEntries().get(i).getRequestEntry()))
                    .isEqualTo(
                            deserializedState.getBufferedRequestEntries().get(i).getRequestEntry());
        }
    }
}
