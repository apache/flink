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

package org.apache.flink.connector.base.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DemultiplexingSink}. */
class DemultiplexingSinkTest {

    @Test
    void testSinkCreation() {
        final TestSinkRouter router = new TestSinkRouter();
        final DemultiplexingSink<String, String> sink = new DemultiplexingSink<>(router);

        assertThat(sink.getSinkRouter()).isSameAs(router);
    }

    @Test
    void testSinkCreationWithNullRouter() {
        assertThatThrownBy(() -> new DemultiplexingSink<String, String>(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("sinkRouter must not be null");
    }

    @Test
    void testCreateWriter() throws IOException {
        final TestSinkRouter router = new TestSinkRouter();
        final DemultiplexingSink<String, String> sink = new DemultiplexingSink<>(router);
        final TestSinkInitContext context = new TestSinkInitContext();

        final SinkWriter<String> writer = sink.createWriter(context);

        assertThat(writer).isInstanceOf(DemultiplexingSinkWriter.class);
    }

    @Test
    void testWriterStateSerializer() {
        final TestSinkRouter router = new TestSinkRouter();
        final DemultiplexingSink<String, String> sink = new DemultiplexingSink<>(router);

        assertThat(sink.getWriterStateSerializer()).isNotNull();
        assertThat(sink.getWriterStateSerializer())
                .isInstanceOf(DemultiplexingSinkStateSerializer.class);
    }

    @Test
    void testRestoreWriter() {
        final TestSinkRouter router = new TestSinkRouter();
        final DemultiplexingSink<String, String> sink = new DemultiplexingSink<>(router);
        final TestSinkInitContext context = new TestSinkInitContext();

        // Create some state to restore from
        final DemultiplexingSinkState<String> state1 = new DemultiplexingSinkState<>();
        state1.setRouteState("a", new byte[] {1, 2, 3});
        state1.setRouteState("b", new byte[] {4, 5, 6});

        final DemultiplexingSinkState<String> state2 = new DemultiplexingSinkState<>();
        state2.setRouteState("c", new byte[] {7, 8, 9});

        final java.util.List<DemultiplexingSinkState<String>> recoveredStates =
                java.util.Arrays.asList(state1, state2);

        // Restore the writer with the states
        final var restoredWriter = sink.restoreWriter(context, recoveredStates);

        // Verify that the restored writer is the correct type
        assertThat(restoredWriter).isInstanceOf(DemultiplexingSinkWriter.class);

        // Verify that the writer was created successfully
        assertThat(restoredWriter).isNotNull();
    }

    @Test
    void testRestoreWriterWithEmptyState() {
        final TestSinkRouter router = new TestSinkRouter();
        final DemultiplexingSink<String, String> sink = new DemultiplexingSink<>(router);
        final TestSinkInitContext context = new TestSinkInitContext();

        // Create an empty state
        final DemultiplexingSinkState<String> emptyState = new DemultiplexingSinkState<>();
        final java.util.List<DemultiplexingSinkState<String>> recoveredStates = List.of(emptyState);

        // Restore the writer with empty state
        final var restoredWriter = sink.restoreWriter(context, recoveredStates);

        // Verify that the restored writer is created successfully even with empty state
        assertThat(restoredWriter).isNotNull();
        assertThat(restoredWriter).isInstanceOf(DemultiplexingSinkWriter.class);
    }

    /** Test implementation of {@link SinkRouter}. */
    private static class TestSinkRouter implements SinkRouter<String, String> {
        private final AtomicInteger sinkCreationCount = new AtomicInteger(0);

        @Override
        public String getRoute(String element) {
            // Route based on first character
            return element.substring(0, 1);
        }

        @Override
        public Sink<String> createSink(String route, String element) {
            sinkCreationCount.incrementAndGet();
            return new TestSink(route);
        }

        public int getSinkCreationCount() {
            return sinkCreationCount.get();
        }
    }

    /** Test implementation of {@link Sink}. */
    private static class TestSink implements Sink<String> {
        private final String route;

        public TestSink(String route) {
            this.route = route;
        }

        @Override
        public SinkWriter<String> createWriter(
                org.apache.flink.api.connector.sink2.WriterInitContext context) {
            return new TestSinkWriter(route);
        }

        public String getRoute() {
            return route;
        }
    }

    /** Test implementation of {@link SinkWriter}. */
    private static class TestSinkWriter implements SinkWriter<String> {
        private final String route;
        private final List<String> elements = new ArrayList<>();
        private boolean closed = false;

        public TestSinkWriter(String route) {
            this.route = route;
        }

        @Override
        public void write(String element, Context context) {
            if (closed) {
                throw new IllegalStateException("Writer is closed");
            }
            elements.add(element);
        }

        @Override
        public void flush(boolean endOfInput) {
            // No-op for test
        }

        @Override
        public void close() {
            closed = true;
        }

        public List<String> getElements() {
            return new ArrayList<>(elements);
        }

        public String getRoute() {
            return route;
        }

        public boolean isClosed() {
            return closed;
        }
    }
}
