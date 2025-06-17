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

package org.apache.flink.connector.base.sink.writer;

import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.assertThatBufferStatesAreEqual;
import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.getTestState;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test class for {@link AsyncSinkWriterStateSerializer}. */
class AsyncSinkWriterStateSerializerTest {

    private static final int ESTIMATED_ELEMENT_SIZE = 7;

    @Test
    void testSerializeAndDeSerialize() throws IOException {
        AsyncSinkWriterStateSerializerImpl stateSerializer =
                new AsyncSinkWriterStateSerializerImpl();
        BufferedRequestState<String> state =
                getTestState((element, context) -> element, String::length);
        BufferedRequestState<String> deserializedState =
                stateSerializer.deserialize(0, stateSerializer.serialize(state));

        assertThatBufferStatesAreEqual(state, deserializedState);
    }

    private static class AsyncSinkWriterStateSerializerImpl
            extends AsyncSinkWriterStateSerializer<String> {

        @Override
        protected void serializeRequestToStream(String request, DataOutputStream out)
                throws IOException {
            out.write(request.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        protected String deserializeRequestFromStream(long requestSize, DataInputStream in)
                throws IOException {
            byte[] requestData = new byte[(int) requestSize];
            in.read(requestData);
            return new String(requestData, StandardCharsets.UTF_8);
        }

        @Override
        public int getVersion() {
            return 1;
        }
    }

    @Test
    void testOnlyEstimatedSizeCausesDeserializedError() {
        // AsyncSinkWriter provides getSizeInBytes to get the size of request entry. Sometimes, get
        // the accurate size has a lot of overhead. So the implementation of connector need to
        // estimate the size. This test case selects 7 as the estimated value. You will see the
        // error while deserializing the state.
        AsyncSinkWriterStateSerializerImpl stateSerializer =
                new AsyncSinkWriterStateSerializerImpl();
        BufferedRequestState<String> state =
                getTestState((element, context) -> element, (element) -> ESTIMATED_ELEMENT_SIZE);
        assertThatThrownBy(() -> stateSerializer.deserialize(0, stateSerializer.serialize(state)))
                .isInstanceOf(Throwable.class);
    }

    @Test
    void testEstimatedSize() throws IOException {
        // This test case adopts the estimated value 7 too. And override getSerializedSizeInBytes to
        // get the real serialized size.
        AsyncSinkWriterStateSerializerImpl2 stateSerializer =
                new AsyncSinkWriterStateSerializerImpl2();
        BufferedRequestState<String> state =
                getTestState((element, context) -> element, String::length);
        BufferedRequestState<String> estimatedState =
                getTestState((element, context) -> element, (element) -> ESTIMATED_ELEMENT_SIZE);
        BufferedRequestState<String> deserializedState =
                stateSerializer.deserialize(0, stateSerializer.serialize(estimatedState));

        assertThatBufferStatesAreEqual(state, deserializedState);
    }

    private static class AsyncSinkWriterStateSerializerImpl2
            extends AsyncSinkWriterStateSerializerImpl {

        @Override
        protected long getSerializedSizeInBytes(RequestEntryWrapper<String> wrapper) {
            return wrapper.getRequestEntry().length();
        }

        @Override
        public int getVersion() {
            return 1;
        }
    }
}
