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
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
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
        DummySinkWriter sinkWriter = router.getSinkWriter("a");
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
        private final List<DummySink> createdSinks = new ArrayList<>();

        @Override
        public String getRoute(String element) {
            // Route based on first character
            return element.substring(0, 1);
        }

        @Override
        public Sink<String> createSink(String route, String element) {
            sinkCreationCount.incrementAndGet();
            DummySink sink = new DummySink(route);
            createdSinks.add(sink);
            return sink;
        }

        public int getSinkCreationCount() {
            return sinkCreationCount.get();
        }

        public DummySinkWriter getSinkWriter(String route) {
            return createdSinks.stream()
                    .filter(sink -> sink.getRoute().equals(route))
                    .findFirst()
                    .map(DummySink::getCreatedWriter)
                    .orElse(null);
        }
    }

    /** Test implementation of {@link Sink}. */
    private static class DummySink implements Sink<String> {
        private final String route;
        private DummySinkWriter createdWriter;

        public DummySink(String route) {
            this.route = route;
        }

        @Override
        public SinkWriter<String> createWriter(
                org.apache.flink.api.connector.sink2.WriterInitContext context) {
            createdWriter = new DummySinkWriter(route);
            return createdWriter;
        }

        public String getRoute() {
            return route;
        }

        public DummySinkWriter getCreatedWriter() {
            return createdWriter;
        }
    }

    /** Test implementation of {@link SinkWriter}. */
    private static class DummySinkWriter implements SinkWriter<String> {
        private final String route;
        private final List<String> elements = new ArrayList<>();
        private final List<Watermark> watermarksReceived = new ArrayList<>();
        private int flushCount = 0;
        private boolean closed = false;

        public DummySinkWriter(String route) {
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
}
