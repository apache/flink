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

package org.apache.flink.connector.firehose.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.firehose.model.Record;

import java.io.IOException;

import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.assertThatBufferStatesAreEqual;
import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.getTestState;

/** Test class for {@link KinesisFirehoseStateSerializer}. */
class KinesisFirehoseStateSerializerTest {

    private static final ElementConverter<String, Record> ELEMENT_CONVERTER =
            KinesisFirehoseSinkElementConverter.<String>builder()
                    .setSerializationSchema(new SimpleStringSchema())
                    .build();

    @Test
    void testSerializeAndDeserialize() throws IOException {
        BufferedRequestState<Record> expectedState =
                getTestState(ELEMENT_CONVERTER, this::getRequestSize);

        KinesisFirehoseStateSerializer serializer = new KinesisFirehoseStateSerializer();
        BufferedRequestState<Record> actualState =
                serializer.deserialize(1, serializer.serialize(expectedState));

        assertThatBufferStatesAreEqual(actualState, expectedState);
    }

    private int getRequestSize(Record requestEntry) {
        return requestEntry.data().asByteArrayUnsafe().length;
    }
}
