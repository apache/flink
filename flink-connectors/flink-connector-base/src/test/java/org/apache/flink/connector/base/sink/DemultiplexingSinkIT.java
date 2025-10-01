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

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link DemultiplexingSink}. */
class DemultiplexingSinkIT {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

    @Test
    void testBasicElementRouting() throws Exception {
        // Create a simple router that routes by first character
        final TestSinkRouter router = new TestSinkRouter();

        // Create the demultiplexing sink
        final DemultiplexingSink<String, String> demuxSink = new DemultiplexingSink<>(router);

        // Create a data stream with elements that will route to different sinks
        env.fromElements("apple", "banana", "apricot", "blueberry", "avocado", "cherry")
                .sinkTo(demuxSink);

        // Execute the job
        env.execute("Integration Test: DemultiplexingSinkIT");

        // Verify results (should contain three routes: "a", "b", "c")
        ConcurrentMap<String, List<String>> results = TestSinkRouter.getResults();
        assertThat(results).hasSize(3);
        assertThat(results.get("a")).containsExactlyInAnyOrder("apple", "apricot", "avocado");
        assertThat(results.get("b")).containsExactlyInAnyOrder("banana", "blueberry");
        assertThat(results.get("c")).containsExactlyInAnyOrder("cherry");
    }

    /** A serializable test router that routes by first character. */
    private static class TestSinkRouter implements SinkRouter<String, String> {
        private static final long serialVersionUID = 1L;

        // Static shared results map for collecting test results
        private static final ConcurrentMap<String, List<String>> results =
                new ConcurrentHashMap<>();

        @Override
        public String getRoute(String element) {
            return element.substring(0, 1);
        }

        @Override
        public org.apache.flink.api.connector.sink2.Sink<String> createSink(
                String route, String element) {
            return new CollectingSink(route, results);
        }

        public static ConcurrentMap<String, List<String>> getResults() {
            return results;
        }

        public static void clearResults() {
            results.clear();
        }
    }

    /** A simple collecting sink for testing. */
    private static class CollectingSink
            implements org.apache.flink.api.connector.sink2.Sink<String> {
        private static final long serialVersionUID = 1L;

        private final String route;
        private final ConcurrentMap<String, List<String>> results;

        public CollectingSink(String route, ConcurrentMap<String, List<String>> results) {
            this.route = route;
            this.results = results;
        }

        @Override
        public org.apache.flink.api.connector.sink2.SinkWriter<String> createWriter(
                org.apache.flink.api.connector.sink2.WriterInitContext context) {
            return new CollectingSinkWriter(route, results);
        }
    }

    /** A simple collecting sink writer for testing. */
    private static class CollectingSinkWriter
            implements org.apache.flink.api.connector.sink2.SinkWriter<String> {
        private static final long serialVersionUID = 1L;

        private final String route;
        private final ConcurrentMap<String, List<String>> results;

        public CollectingSinkWriter(String route, ConcurrentMap<String, List<String>> results) {
            this.route = route;
            this.results = results;
        }

        @Override
        public void write(String element, Context context) {
            results.computeIfAbsent(route, k -> new ArrayList<>()).add(element);
        }

        @Override
        public void flush(boolean endOfInput) {
            // No-op for this test
        }

        @Override
        public void close() {
            // No-op for this test
        }
    }
}
