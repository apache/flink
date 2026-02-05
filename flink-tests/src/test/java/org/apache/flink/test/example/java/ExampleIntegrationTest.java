/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.example.java;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Simple flink job test with {@link MiniClusterWithClientResource}. Please ensure that any changes
 * made here are also updated in the Flink documentation.
 *
 * @see <a
 *     href="https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/testing/#testing-flink-jobs">Testing
 *     Flink Jobs</a>
 */
public class ExampleIntegrationTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void testIncrementPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(2);

        // values are collected in a static variable
        CollectSink.values.clear();

        // create a stream of custom elements and apply transformations
        env.fromData(1L, 21L, 22L).map(new IncrementMapFunction()).sinkTo(new CollectSink());

        // execute
        env.execute();

        // verify your results
        assertTrue(CollectSink.values.containsAll(List.of(2L, 22L, 23L)));
    }

    // create a testing sink
    private static class CollectSink implements Sink<Long> {

        // must be static
        private static final List<Long> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public SinkWriter<Long> createWriter(WriterInitContext context) {
            return new SinkWriter<>() {
                @Override
                public void write(Long element, Context context) {
                    values.add(element);
                }

                @Override
                public void close() {
                    // noting to do here
                }

                @Override
                public void flush(boolean endOfInput) {
                    // noting to do here
                }
            };
        }
    }

    private static class IncrementMapFunction implements MapFunction<Long, Long> {
        @Override
        public Long map(Long record) throws Exception {
            return record + 1;
        }
    }
}
