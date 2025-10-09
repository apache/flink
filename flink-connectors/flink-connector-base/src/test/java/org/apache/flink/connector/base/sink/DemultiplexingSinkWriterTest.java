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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DemultiplexingSinkWriter}. */
class DemultiplexingSinkWriterTest {

    private TestSinkRouter router;
    private TestSinkInitContext context;
    private DemultiplexingSinkWriter<String, String> writer;

    @BeforeEach
    void setUp() {
        router = new TestSinkRouter();
        context = new TestSinkInitContext();
        writer = new DemultiplexingSinkWriter<>(router, context);
    }

    @Test
    void testWriteToSingleRoute() throws IOException, InterruptedException {
        // Write elements that all route to the same destination
        writer.write("apple", createContext());
        writer.write("avocado", createContext());
        writer.write("apricot", createContext());

        // Should have created only one sink
        assertThat(router.getSinkCreationCount()).isEqualTo(1);
        assertThat(writer.getActiveSinkWriterCount()).isEqualTo(1);
        assertThat(writer.getActiveRoutes()).containsExactly("a");

        // All elements should be in the same sink writer
        TestSinkWriter sinkWriter = router.getSinkWriter("a");
        assertThat(sinkWriter.getElements()).containsExactly("apple", "avocado", "apricot");
    }

    @Test
    void testWriteToMultipleRoutes() throws IOException, InterruptedException {
        // Write elements that route to different destinations ("apple" and "apricot" should resolve
        // to the same)
        writer.write("apple", createContext());
        writer.write("banana", createContext());
        writer.write("cherry", createContext());
        writer.write("apricot", createContext());

        // Should have created three sinks (a, b, c)
        assertThat(router.getSinkCreationCount()).isEqualTo(3);
        assertThat(writer.getActiveSinkWriterCount()).isEqualTo(3);
        assertThat(writer.getActiveRoutes()).containsExactlyInAnyOrder("a", "b", "c");

        // Check elements are routed correctly
        assertThat(router.getSinkWriter("a").getElements()).containsExactly("apple", "apricot");
        assertThat(router.getSinkWriter("b").getElements()).containsExactly("banana");
        assertThat(router.getSinkWriter("c").getElements()).containsExactly("cherry");
    }

    @Test
    void testFlush() throws IOException, InterruptedException {
        // Write to multiple routes
        writer.write("apple", createContext());
        writer.write("banana", createContext());

        // Flush should be called on all sink writers
        writer.flush(false);

        // Verify flush was called (our test implementation tracks this)
        assertThat(router.getSinkWriter("a").getFlushCount()).isEqualTo(1);
        assertThat(router.getSinkWriter("b").getFlushCount()).isEqualTo(1);
    }

    @Test
    void testWriteWatermark() throws IOException, InterruptedException {
        // Write to multiple routes
        writer.write("apple", createContext());
        writer.write("banana", createContext());

        // Write watermark should be propagated to all sink writers
        Watermark watermark = new Watermark(12345L);
        writer.writeWatermark(watermark);

        // Verify watermark was written (our test implementation tracks this)
        assertThat(router.getSinkWriter("a").getWatermarksReceived()).containsExactly(watermark);
        assertThat(router.getSinkWriter("b").getWatermarksReceived()).containsExactly(watermark);
    }

    @Test
    void testClose() throws Exception {
        // Write to multiple routes
        writer.write("apple", createContext());
        writer.write("banana", createContext());

        // Close should close all sink writers
        writer.close();

        // Verify all writers were closed
        assertThat(router.getSinkWriter("a").isClosed()).isTrue();
        assertThat(router.getSinkWriter("b").isClosed()).isTrue();

        // Active count should be zero after close
        assertThat(writer.getActiveSinkWriterCount()).isEqualTo(0);
    }

    @Test
    void testSnapshotState() throws IOException, InterruptedException {
        // Write to multiple routes
        writer.write("apple", createContext());
        writer.write("banana", createContext());

        // Snapshot state
        List<DemultiplexingSinkState<String>> states = writer.snapshotState(1L);

        // Should return state (even if empty for our test implementation)
        assertThat(states).isNotNull();
    }

    @Test
    void testJavaSerializationFallback() throws Exception {
        // Create a router that creates sinks without proper state serializers
        JavaSerializationFallbackRouter fallbackRouter = new JavaSerializationFallbackRouter();
        DemultiplexingSinkWriter<String, String> writer =
                new DemultiplexingSinkWriter<>(fallbackRouter, context);

        // Write elements to create stateful sink writers
        writer.write("apple", createContext());
        writer.write("banana", createContext());

        // Verify sink writers were created
        assertThat(writer.getActiveSinkWriterCount()).isEqualTo(2);

        // Add some state to the stateful sink writers
        JavaSerializationFallbackSink sinkA = fallbackRouter.getCreatedSink("a");
        JavaSerializationFallbackSink sinkB = fallbackRouter.getCreatedSink("b");

        assertThat(sinkA).isNotNull();
        assertThat(sinkB).isNotNull();

        // The stateful writers should have received the elements
        assertThat(sinkA.getCreatedWriter().getElements()).containsExactly("apple");
        assertThat(sinkB.getCreatedWriter().getElements()).containsExactly("banana");

        // Snapshot state - this should trigger Java serialization fallback
        // since our test sink has a failing state serializer
        List<DemultiplexingSinkState<String>> snapshotStates = writer.snapshotState(1L);

        // Should have successfully created state using Java serialization fallback
        assertThat(snapshotStates).hasSize(1);
        DemultiplexingSinkState<String> state = snapshotStates.get(0);
        assertThat(state.size()).isEqualTo(2);
        assertThat(state.getRoutes()).containsExactlyInAnyOrder("a", "b");

        writer.close();
    }

    /** Test implementation of {@link SinkWriter.Context}. */
    private SinkWriter.Context createContext() {
        return new SinkWriter.Context() {
            @Override
            public long currentWatermark() {
                return 0;
            }

            @Override
            public Long timestamp() {
                return null;
            }
        };
    }

    /** Test implementation of {@link SinkRouter}. */
    private static class TestSinkRouter implements SinkRouter<String, String> {
        private final AtomicInteger sinkCreationCount = new AtomicInteger(0);
        private final List<TestSink> createdSinks = new ArrayList<>();

        @Override
        public String getRoute(String element) {
            // Route based on first character
            return element.substring(0, 1);
        }

        @Override
        public Sink<String> createSink(String route, String element) {
            sinkCreationCount.incrementAndGet();
            TestSink sink = new TestSink(route);
            createdSinks.add(sink);
            return sink;
        }

        public int getSinkCreationCount() {
            return sinkCreationCount.get();
        }

        public TestSinkWriter getSinkWriter(String route) {
            return createdSinks.stream()
                    .filter(sink -> sink.getRoute().equals(route))
                    .findFirst()
                    .map(TestSink::getCreatedWriter)
                    .orElse(null);
        }
    }

    /** Test implementation of {@link Sink}. */
    private static class TestSink implements Sink<String> {
        private final String route;
        private TestSinkWriter createdWriter;

        public TestSink(String route) {
            this.route = route;
        }

        @Override
        public SinkWriter<String> createWriter(
                org.apache.flink.api.connector.sink2.WriterInitContext context) {
            createdWriter = new TestSinkWriter(route);
            return createdWriter;
        }

        public String getRoute() {
            return route;
        }

        public TestSinkWriter getCreatedWriter() {
            return createdWriter;
        }
    }

    /** Test implementation of {@link SinkWriter}. */
    private static class TestSinkWriter implements SinkWriter<String> {
        private final String route;
        private final List<String> elements = new ArrayList<>();
        private final List<Watermark> watermarksReceived = new ArrayList<>();
        private int flushCount = 0;
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
            flushCount++;
        }

        @Override
        public void writeWatermark(Watermark watermark) {
            watermarksReceived.add(watermark);
        }

        @Override
        public void close() {
            closed = true;
        }

        public List<String> getElements() {
            return new ArrayList<>(elements);
        }

        public List<Watermark> getWatermarksReceived() {
            return new ArrayList<>(watermarksReceived);
        }

        public int getFlushCount() {
            return flushCount;
        }

        public String getRoute() {
            return route;
        }

        public boolean isClosed() {
            return closed;
        }
    }

    /** Test serialization fallback implementation of {@link SinkRouter}. */
    private static class JavaSerializationFallbackRouter implements SinkRouter<String, String> {
        private final java.util.Map<String, JavaSerializationFallbackSink> createdSinks =
                new java.util.HashMap<>();

        @Override
        public String getRoute(String element) {
            return element.substring(0, 1);
        }

        @Override
        public Sink<String> createSink(String route, String element) {
            JavaSerializationFallbackSink sink = new JavaSerializationFallbackSink(route);
            createdSinks.put(route, sink);
            return sink;
        }

        public JavaSerializationFallbackSink getCreatedSink(String route) {
            return createdSinks.get(route);
        }
    }

    /** Test serialization fallback implementation of {@link Sink}. */
    private static class JavaSerializationFallbackSink
            implements Sink<String>, SupportsWriterState<String, String> {
        private final String route;
        private JavaSerializationFallbackWriter createdWriter;

        public JavaSerializationFallbackSink(String route) {
            this.route = route;
        }

        @Override
        public SinkWriter<String> createWriter(WriterInitContext context) {
            createdWriter = new JavaSerializationFallbackWriter(route);
            return createdWriter;
        }

        @Override
        public StatefulSinkWriter<String, String> restoreWriter(
                WriterInitContext context, Collection<String> recoveredState) {
            createdWriter = new JavaSerializationFallbackWriter(route);
            // Restore the state
            for (String state : recoveredState) {
                createdWriter.addElement(state);
            }
            return createdWriter;
        }

        @Override
        public SimpleVersionedSerializer<String> getWriterStateSerializer() {
            // Return a serializer that will fail, forcing fallback to Java serialization
            return new SimpleVersionedSerializer<String>() {
                @Override
                public int getVersion() {
                    return 1;
                }

                @Override
                public byte[] serialize(String obj) throws IOException {
                    throw new IOException("Intentional serialization failure to test fallback");
                }

                @Override
                public String deserialize(int version, byte[] serialized) throws IOException {
                    throw new IOException("Intentional deserialization failure to test fallback");
                }
            };
        }

        public JavaSerializationFallbackWriter getCreatedWriter() {
            return createdWriter;
        }
    }

    /** Test serialization fallback implementation of {@link StatefulSinkWriter}. */
    private static class JavaSerializationFallbackWriter
            implements StatefulSinkWriter<String, String> {
        private final String route;
        private final List<String> elements = new ArrayList<>();

        public JavaSerializationFallbackWriter(String route) {
            this.route = route;
        }

        @Override
        public void write(String element, Context context) {
            elements.add(element);
        }

        @Override
        public void flush(boolean endOfInput) {
            // No-op for test
        }

        @Override
        public void close() {
            // No-op for test
        }

        @Override
        public List<String> snapshotState(long checkpointId) {
            // Return the elements as state (this will be Java-serialized as fallback)
            return new ArrayList<>(elements);
        }

        public List<String> getElements() {
            return new ArrayList<>(elements);
        }

        public void addElement(String element) {
            elements.add(element);
        }
    }
}
