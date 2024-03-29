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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.co.CoStreamMap;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/** Test checkpoint cancellation barrier. */
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class StreamTaskCancellationBarrierTest {

    /**
     * This test verifies (for onw input tasks) that the Stream tasks react the following way to
     * receiving a checkpoint cancellation barrier: - send a "decline checkpoint" notification out
     * (to the JobManager) - emit a cancellation barrier downstream.
     */
    @Test
    void testDeclineCallOnCancelBarrierOneInput() throws Exception {

        OneInputStreamTaskTestHarness<String, String> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        1,
                        2,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);
        testHarness.setupOutputForSingletonOperatorChain();

        StreamConfig streamConfig = testHarness.getStreamConfig();
        StreamMap<String, String> mapOperator = new StreamMap<>(new IdentityMap());
        streamConfig.setStreamOperator(mapOperator);
        streamConfig.setOperatorID(new OperatorID());

        StreamMockEnvironment environment = spy(testHarness.createEnvironment());

        // start the task
        testHarness.invoke(environment);
        testHarness.waitForTaskRunning();

        // emit cancellation barriers
        testHarness.processEvent(new CancelCheckpointMarker(2L), 0, 1);
        testHarness.processEvent(new CancelCheckpointMarker(2L), 0, 0);
        testHarness.waitForInputProcessing();

        // the decline call should go to the coordinator
        verify(environment, times(1))
                .declineCheckpoint(
                        eq(2L),
                        argThat(
                                new CheckpointExceptionMatcher(
                                        CheckpointFailureReason
                                                .CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER)));

        // a cancellation barrier should be downstream
        Object result = testHarness.getOutput().poll();
        assertThat(result).as("nothing emitted").isNotNull();
        assertThat(result).as("wrong type emitted").isInstanceOf(CancelCheckpointMarker.class);
        assertThat(((CancelCheckpointMarker) result).getCheckpointId())
                .as("wrong checkpoint id")
                .isEqualTo(2L);

        // cancel and shutdown
        testHarness.endInput();
        testHarness.waitForTaskCompletion();
    }

    /**
     * This test verifies (for two input tasks) that the Stream tasks react the following way to
     * receiving a checkpoint cancellation barrier: - send a "decline checkpoint" notification out
     * (to the JobManager) - emit a cancellation barrier downstream.
     */
    @Test
    void testDeclineCallOnCancelBarrierTwoInputs() throws Exception {

        TwoInputStreamTaskTestHarness<String, String, String> testHarness =
                new TwoInputStreamTaskTestHarness<>(
                        TwoInputStreamTask::new,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);
        testHarness.setupOutputForSingletonOperatorChain();

        StreamConfig streamConfig = testHarness.getStreamConfig();
        CoStreamMap<String, String, String> op = new CoStreamMap<>(new UnionCoMap());
        streamConfig.setStreamOperator(op);
        streamConfig.setOperatorID(new OperatorID());

        StreamMockEnvironment environment = spy(testHarness.createEnvironment());

        // start the task
        testHarness.invoke(environment);
        testHarness.waitForTaskRunning();

        // emit cancellation barriers
        testHarness.processEvent(new CancelCheckpointMarker(2L), 0, 0);
        testHarness.processEvent(new CancelCheckpointMarker(2L), 1, 0);
        testHarness.waitForInputProcessing();

        // the decline call should go to the coordinator
        verify(environment, times(1))
                .declineCheckpoint(
                        eq(2L),
                        argThat(
                                new CheckpointExceptionMatcher(
                                        CheckpointFailureReason
                                                .CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER)));

        // a cancellation barrier should be downstream
        Object result = testHarness.getOutput().poll();
        assertThat(result).as("nothing emitted").isNotNull();
        assertThat(result).as("wrong type emitted").isInstanceOf(CancelCheckpointMarker.class);
        assertThat(((CancelCheckpointMarker) result).getCheckpointId())
                .as("wrong checkpoint id")
                .isEqualTo(2L);

        // cancel and shutdown
        testHarness.endInput();
        testHarness.waitForTaskCompletion();
    }

    private static class IdentityMap implements MapFunction<String, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String map(String value) throws Exception {
            return value;
        }
    }

    private static class UnionCoMap implements CoMapFunction<String, String, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String map1(String value) throws Exception {
            return value;
        }

        @Override
        public String map2(String value) throws Exception {
            return value;
        }
    }

    /** A validation matcher for checkpoint exception against failure reason. */
    private static class CheckpointExceptionMatcher extends BaseMatcher<CheckpointException> {

        private final CheckpointFailureReason failureReason;

        private CheckpointExceptionMatcher(CheckpointFailureReason failureReason) {
            this.failureReason = failureReason;
        }

        @Override
        public boolean matches(Object o) {
            return o != null
                    && o.getClass() == CheckpointException.class
                    && ((CheckpointException) o).getCheckpointFailureReason().equals(failureReason);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("CheckpointException - reason = " + failureReason);
        }
    }
}
