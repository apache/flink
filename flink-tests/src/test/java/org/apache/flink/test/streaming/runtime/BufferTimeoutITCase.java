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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBaseJUnit4;
import org.apache.flink.test.util.source.AbstractTestSource;
import org.apache.flink.test.util.source.SingleSplitEnumerator;
import org.apache.flink.test.util.source.TestSourceReader;
import org.apache.flink.test.util.source.TestSplit;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertTrue;

/** Tests for {@link StreamExecutionEnvironment#setBufferTimeout(long)}. */
public class BufferTimeoutITCase extends AbstractTestBaseJUnit4 {

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

        env.fromSource(
                        createSingleElementIdleSource(),
                        WatermarkStrategy.<Integer>noWatermarks(),
                        "v2-src")
                .slotSharingGroup("source")
                .sinkTo(createResultCollectingSink(results))
                .slotSharingGroup("sink");

        final JobClient jobClient = env.executeAsync();
        CommonTestUtils.waitForAllTaskRunning(
                MINI_CLUSTER_RESOURCE.getMiniCluster(), jobClient.getJobID(), false);

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

    /** Sink V2 that collects results in shared reference. */
    private static Sink<Integer> createResultCollectingSink(
            SharedReference<ArrayList<Integer>> results) {
        return context ->
                new SinkWriter<>() {
                    @Override
                    public void write(Integer element, Context ctx) {
                        results.get().add(element);
                    }

                    @Override
                    public void flush(boolean endOfInput) {}

                    @Override
                    public void close() {}
                };
    }

    /** Source V2 that emits one element then stays idle (unbounded). */
    private static AbstractTestSource<Integer> createSingleElementIdleSource() {
        return new AbstractTestSource<Integer>() {
            @Override
            public SourceReader<Integer, TestSplit> createReader(SourceReaderContext ctx) {
                return new TestSourceReader<>(ctx) {
                    private boolean emitted = false;

                    @Override
                    public InputStatus pollNext(ReaderOutput<Integer> out) {
                        if (!emitted) {
                            out.collect(1);
                            emitted = true;
                        }
                        return InputStatus.NOTHING_AVAILABLE;
                    }
                };
            }

            @Override
            public SplitEnumerator<TestSplit, Void> createEnumerator(
                    SplitEnumeratorContext<TestSplit> context) {
                return new SingleSplitEnumerator(context);
            }

            @Override
            public Boundedness getBoundedness() {
                return Boundedness.CONTINUOUS_UNBOUNDED;
            }
        };
    }
}
