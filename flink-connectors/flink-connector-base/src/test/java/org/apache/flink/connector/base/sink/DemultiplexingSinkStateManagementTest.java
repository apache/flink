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
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for state management in {@link DemultiplexingSinkWriter}. */
class DemultiplexingSinkStateManagementTest {

    private TestStatefulSinkRouter router;
    private TestSinkInitContext context;

    @BeforeEach
    void setUp() {
        router = new TestStatefulSinkRouter();
        context = new TestSinkInitContext();
    }

    @Test
    void testSnapshotAndRestoreState() throws Exception {
        // Create writer and write to multiple routes
        DemultiplexingSinkWriter<String, String> writer =
                new DemultiplexingSinkWriter<>(router, context);

        // Write elements to create sink writers ("apple" and "apricot" resolve to same sink)
        writer.write("apple", createContext());
        writer.write("banana", createContext());
        writer.write("apricot", createContext());

        // Verify sink writers were created
        assertThat(writer.getActiveSinkWriterCount()).isEqualTo(2);
        assertThat(writer.getActiveRoutes()).containsExactlyInAnyOrder("a", "b");

        // Add some state to the sink writers
        TestStatefulSinkWriter writerA = router.getStatefulSinkWriter("a");
        TestStatefulSinkWriter writerB = router.getStatefulSinkWriter("b");
        writerA.addState("state-a-1");
        writerA.addState("state-a-2");
        writerB.addState("state-b-1");

        // Verify state was added
        assertThat(writerA.getStates()).containsExactly("state-a-1", "state-a-2");
        assertThat(writerB.getStates()).containsExactly("state-b-1");

        // Snapshot state
        List<DemultiplexingSinkState<String>> snapshotStates = writer.snapshotState(1L);
        assertThat(snapshotStates).hasSize(1);

        DemultiplexingSinkState<String> state = snapshotStates.get(0);
        assertThat(state.getRoutes()).containsExactlyInAnyOrder("a", "b");

        // Close the original writer
        writer.close();

        // Create a new router for the restored writer to avoid confusion with old sinks
        TestStatefulSinkRouter restoredRouter = new TestStatefulSinkRouter();

        // Create a new writer with restored state
        DemultiplexingSinkWriter<String, String> restoredWriter =
                new DemultiplexingSinkWriter<>(restoredRouter, context, snapshotStates);

        // Write to the same routes to trigger restoration ("avocado" -> "a", "blueberry" -> "b")
        restoredWriter.write("avocado", createContext());
        restoredWriter.write("blueberry", createContext());

        // Verify that state was restored
        TestStatefulSinkWriter restoredWriterA = restoredRouter.getStatefulSinkWriter("a");
        TestStatefulSinkWriter restoredWriterB = restoredRouter.getStatefulSinkWriter("b");

        // Verify that the writers were restored from state
        assertThat(restoredWriterA.wasRestoredFromState()).isTrue();
        assertThat(restoredWriterB.wasRestoredFromState()).isTrue();

        // Verify that the restored state contains the expected data
        assertThat(restoredWriterA.getStates()).containsExactly("state-a-1", "state-a-2");
        assertThat(restoredWriterB.getStates()).containsExactly("state-b-1");

        restoredWriter.close();
    }

    @Test
    void testSnapshotEmptyState() throws Exception {
        DemultiplexingSinkWriter<String, String> writer =
                new DemultiplexingSinkWriter<>(router, context);

        // Snapshot without any active writers
        List<DemultiplexingSinkState<String>> states = writer.snapshotState(1L);
        assertThat(states).isEmpty();

        writer.close();
    }

    @Test
    void testRestoreWithNonStatefulSinks() throws Exception {
        // Use a router that creates non-stateful sinks
        TestNonStatefulSinkRouter nonStatefulRouter = new TestNonStatefulSinkRouter();

        // Create some dummy state
        DemultiplexingSinkState<String> dummyState = new DemultiplexingSinkState<>();
        dummyState.setRouteState("a", new byte[] {1, 2, 3});

        // Create writer with restored state
        DemultiplexingSinkWriter<String, String> writer =
                new DemultiplexingSinkWriter<>(
                        nonStatefulRouter, context, Arrays.asList(dummyState));

        // Write to trigger sink creation
        writer.write("apple", createContext());

        // Should work fine even though the sink doesn't support state
        assertThat(writer.getActiveSinkWriterCount()).isEqualTo(1);

        writer.close();
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

    /** Test router that creates stateful sinks. */
    private static class TestStatefulSinkRouter implements SinkRouter<String, String> {
        private final AtomicInteger sinkCreationCount = new AtomicInteger(0);
        private final List<TestStatefulSink> createdSinks = new ArrayList<>();

        @Override
        public String getRoute(String element) {
            return element.substring(0, 1);
        }

        @Override
        public Sink<String> createSink(String route, String element) {
            sinkCreationCount.incrementAndGet();
            TestStatefulSink sink = new TestStatefulSink(route);
            createdSinks.add(sink);
            return sink;
        }

        public TestStatefulSinkWriter getStatefulSinkWriter(String route) {
            return createdSinks.stream()
                    .filter(sink -> sink.getRoute().equals(route))
                    .findFirst()
                    .map(TestStatefulSink::getCreatedWriter)
                    .orElse(null);
        }
    }

    /** Test router that creates non-stateful sinks. */
    private static class TestNonStatefulSinkRouter implements SinkRouter<String, String> {
        @Override
        public String getRoute(String element) {
            return element.substring(0, 1);
        }

        @Override
        public Sink<String> createSink(String route, String element) {
            return new TestNonStatefulSink(route);
        }
    }

    /** Test stateful sink implementation. */
    private static class TestStatefulSink
            implements Sink<String>, SupportsWriterState<String, String> {
        private final String route;
        private TestStatefulSinkWriter createdWriter;

        public TestStatefulSink(String route) {
            this.route = route;
        }

        @Override
        public SinkWriter<String> createWriter(WriterInitContext context) {
            createdWriter = new TestStatefulSinkWriter(route);
            return createdWriter;
        }

        @Override
        public StatefulSinkWriter<String, String> restoreWriter(
                WriterInitContext context, Collection<String> recoveredState) {
            createdWriter = new TestStatefulSinkWriter(route, recoveredState);
            return createdWriter;
        }

        @Override
        public SimpleVersionedSerializer<String> getWriterStateSerializer() {
            return new TestStringSerializer();
        }

        public String getRoute() {
            return route;
        }

        public TestStatefulSinkWriter getCreatedWriter() {
            return createdWriter;
        }
    }

    /** Test non-stateful sink implementation. */
    private static class TestNonStatefulSink implements Sink<String> {
        private final String route;

        public TestNonStatefulSink(String route) {
            this.route = route;
        }

        @Override
        public SinkWriter<String> createWriter(WriterInitContext context) {
            return new TestNonStatefulSinkWriter(route);
        }
    }

    /** Test stateful sink writer. */
    private static class TestStatefulSinkWriter implements StatefulSinkWriter<String, String> {
        private final String route;
        private final List<String> elements = new ArrayList<>();
        private final List<String> states = new ArrayList<>();
        private final boolean restoredFromState;

        public TestStatefulSinkWriter(String route) {
            this.route = route;
            this.restoredFromState = false;
        }

        public TestStatefulSinkWriter(String route, Collection<String> recoveredStates) {
            this.route = route;
            this.states.addAll(recoveredStates);
            this.restoredFromState = true;
        }

        @Override
        public void write(String element, Context context) {
            elements.add(element);
        }

        @Override
        public void flush(boolean endOfInput) {
            // No-op
        }

        @Override
        public void close() {
            // No-op
        }

        @Override
        public List<String> snapshotState(long checkpointId) {
            return new ArrayList<>(states);
        }

        public void addState(String state) {
            states.add(state);
        }

        public boolean wasRestoredFromState() {
            return restoredFromState;
        }

        public List<String> getElements() {
            return new ArrayList<>(elements);
        }

        public List<String> getStates() {
            return new ArrayList<>(states);
        }
    }

    /** Test non-stateful sink writer. */
    private static class TestNonStatefulSinkWriter implements SinkWriter<String> {
        private final String route;
        private final List<String> elements = new ArrayList<>();

        public TestNonStatefulSinkWriter(String route) {
            this.route = route;
        }

        @Override
        public void write(String element, Context context) {
            elements.add(element);
        }

        @Override
        public void flush(boolean endOfInput) {
            // No-op
        }

        @Override
        public void close() {
            // No-op
        }

        public List<String> getElements() {
            return new ArrayList<>(elements);
        }
    }

    /** Simple string serializer for testing. */
    private static class TestStringSerializer implements SimpleVersionedSerializer<String> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(String obj) {
            return obj.getBytes();
        }

        @Override
        public String deserialize(int version, byte[] serialized) {
            return new String(serialized);
        }
    }
}
