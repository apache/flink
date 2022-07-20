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

import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.assertThatBufferStatesAreEqual;
import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.getTestState;

/** Test class for {@link AsyncSinkWriterStateSerializer}. */
public class AsyncSinkWriterStateSerializerTest {

    @Test
    public void testSerializeAndDeSerialize() throws IOException {
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
}
