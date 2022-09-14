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

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link TestProcessingTimeService}. */
public class TestProcessingTimeServiceTest {

    @Test
    public void testCustomTimeServiceProvider() throws Throwable {
        final TestProcessingTimeService tp = new TestProcessingTimeService();

        final OneInputStreamTaskTestHarness<String, String> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        (env) -> new OneInputStreamTask<>(env, tp),
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setupOutputForSingletonOperatorChain();

        StreamConfig streamConfig = testHarness.getStreamConfig();

        StreamMap<String, String> mapOperator =
                new StreamMap<>(new StreamTaskTimerITCase.DummyMapFunction<>());
        streamConfig.setStreamOperator(mapOperator);
        streamConfig.setOperatorID(new OperatorID());

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        ProcessingTimeService processingTimeService =
                ((StreamMap<?, ?>) testHarness.getHeadOperator()).getProcessingTimeService();

        assertEquals(Long.MIN_VALUE, processingTimeService.getCurrentProcessingTime());

        tp.setCurrentTime(11);
        assertEquals(processingTimeService.getCurrentProcessingTime(), 11);

        tp.setCurrentTime(15);
        tp.setCurrentTime(16);
        assertEquals(processingTimeService.getCurrentProcessingTime(), 16);

        // register 2 tasks
        processingTimeService.registerTimer(30, timestamp -> {});

        processingTimeService.registerTimer(40, timestamp -> {});

        assertEquals(2, tp.getNumActiveTimers());

        tp.setCurrentTime(35);
        assertEquals(1, tp.getNumActiveTimers());

        tp.setCurrentTime(40);
        assertEquals(0, tp.getNumActiveTimers());

        tp.shutdownService();
    }
}
