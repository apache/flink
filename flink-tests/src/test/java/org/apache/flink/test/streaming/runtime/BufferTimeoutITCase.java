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

import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.execution.JobClient;
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for {@link StreamExecutionEnvironment#setBufferTimeout(long)}. */
public class BufferTimeoutITCase extends AbstractTestBase {

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    /**
     * The test verifies that it is possible to disable buffer flushing. It emits a single record,
     * which should not fill an entire buffer, thus it should not never reach the sink. We check the
     * sink has not seen any records after 2 times the default buffer timeout.
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
                            public void invoke(Integer value, Context context) throws Exception {
                                results.get().add(value);
                            }
                        })
                .slotSharingGroup("sink");

        final JobClient jobClient = env.executeAsync();
        CommonTestUtils.waitForAllTaskRunning(
                miniClusterResource.getMiniCluster(), jobClient.getJobID(), false);

        Thread.sleep(2 * ExecutionOptions.BUFFER_TIMEOUT.defaultValue().toMillis());
        assertThat(results.get().size(), equalTo(0));
    }
}
