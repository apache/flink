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

package org.apache.flink.api.connector.sink2;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class StatefulSinkBridgeTest {

    @Test
    void statefulSinkIsRecognizedAsSink() {
        TestStatefulSink sink = new TestStatefulSink();
        assertThat(sink).isInstanceOf(Sink.class);
    }

    @Test
    void statefulSinkIsRecognizedAsSupportsWriterState() {
        TestStatefulSink sink = new TestStatefulSink();
        assertThat(sink).isInstanceOf(SupportsWriterState.class);
    }

    @Test
    void statefulSinkWriterExtendsCoreSinkWriter() {
        assertThat(StatefulSinkWriter.class)
                .isAssignableFrom(StatefulSink.StatefulSinkWriter.class);
    }

    @Test
    void withCompatibleStateExtendsCoreWithCompatibleState() {
        assertThat(SupportsWriterState.WithCompatibleState.class)
                .isAssignableFrom(StatefulSink.WithCompatibleState.class);
    }

    private static class TestStatefulSink implements StatefulSink<String, Integer> {

        @Override
        public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
            return new TestStatefulSinkWriter();
        }

        @Override
        public StatefulSinkWriter<String, Integer> restoreWriter(
                WriterInitContext context, Collection<Integer> recoveredState) throws IOException {
            return new TestStatefulSinkWriter();
        }

        @Override
        public SimpleVersionedSerializer<Integer> getWriterStateSerializer() {
            return new SimpleVersionedSerializer<>() {
                @Override
                public int getVersion() {
                    return 0;
                }

                @Override
                public byte[] serialize(Integer obj) {
                    return new byte[0];
                }

                @Override
                public Integer deserialize(int version, byte[] serialized) {
                    return 0;
                }
            };
        }
    }

    private static class TestStatefulSinkWriter
            implements StatefulSink.StatefulSinkWriter<String, Integer> {

        @Override
        public List<Integer> snapshotState(long checkpointId) {
            return Collections.emptyList();
        }

        @Override
        public void write(String element, Context context) {}

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public void close() {}
    }
}
