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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertTrue;

/** Tests for {@link StreamExecutionEnvironment#setBufferTimeout(long)}. */
public class BufferTimeoutITCase extends AbstractTestBase {

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    /**
     * The test verifies that it is possible to disable explicit buffer flushing. It checks that
     * OutputFlasher thread would not be started when the task is running. But this doesn't
     * guarantee that the unfinished buffers can not be flushed by another events.
     */
    @Test
    public void testDisablingBufferTimeout() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setBufferTimeout(-1);
        final SharedReference<ArrayList<Integer>> results = sharedObjects.add(new ArrayList<>());

        env.addSource(
                        new SourceFunction<Integer>() {

                            @Override
                            public void run(SourceContext<Integer> ctx) throws Exception {
                                ctx.collect(1);
                                // just sleep forever
                                Thread.sleep(Long.MAX_VALUE);
                            }

                            @Override
                            public void cancel() {}
                        })
                .slotSharingGroup("source")
                .addSink(
                        new SinkFunction<Integer>() {

                            @Override
                            public void invoke(Integer value, Context context) {
                                results.get().add(value);
                            }
                        })
                .slotSharingGroup("sink");

        final JobClient jobClient = env.executeAsync();
        CommonTestUtils.waitForAllTaskRunning(
                miniClusterResource.getMiniCluster(), jobClient.getJobID(), false);

        assertTrue(
                RecordWriter.DEFAULT_OUTPUT_FLUSH_THREAD_NAME + " thread is unexpectedly running",
                Thread.getAllStackTraces().keySet().stream()
                        .noneMatch(
                                thread ->
                                        thread.getName()
                                                .startsWith(
                                                        RecordWriter
                                                                .DEFAULT_OUTPUT_FLUSH_THREAD_NAME)));
    }
}
