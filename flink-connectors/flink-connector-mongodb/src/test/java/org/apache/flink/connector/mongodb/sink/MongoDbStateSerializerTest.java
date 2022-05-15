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

package org.apache.flink.connector.mongodb.sink;

import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.junit.Test;

import java.io.IOException;

import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.assertThatBufferStatesAreEqual;
import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.getTestState;

/** Test class for {@link MongoDbStateSerializer}. */
public class MongoDbStateSerializerTest {

    private static final ElementConverter<String, MongoDbWriteModel<? extends WriteModel<Document>>>
            ELEMENT_CONVERTER =
                    MongoDbSinkElementConverter.<String>builder()
                            .setMongoDbWriteOperationConverter(
                                    s ->
                                            new MongoDbReplaceOneOperation(
                                                    ImmutableMap.of("filter_key", s),
                                                    ImmutableMap.of("replacement_key", s)))
                            .build();

    @Test
    public void testSerializeAndDeserialize() throws IOException {
        BufferedRequestState<MongoDbWriteModel<? extends WriteModel<Document>>> expectedState =
                getTestState(ELEMENT_CONVERTER, this::getRequestSize);

        MongoDbStateSerializer serializer = new MongoDbStateSerializer();
        BufferedRequestState<MongoDbWriteModel<? extends WriteModel<Document>>> actualState =
                serializer.deserialize(1, serializer.serialize(expectedState));
        assertThatBufferStatesAreEqual(actualState, expectedState);
    }

    private int getRequestSize(MongoDbWriteModel<? extends WriteModel<Document>> requestEntry) {
        return requestEntry.getSizeInBytes();
    }
}
