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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;

import java.util.HashMap;

/** Mock {@link StreamingRuntimeContext} to use in tests. */
public class MockStreamingRuntimeContext extends StreamingRuntimeContext {

    private final boolean isCheckpointingEnabled;

    private final int numParallelSubtasks;
    private final int subtaskIndex;

    public MockStreamingRuntimeContext(
            boolean isCheckpointingEnabled, int numParallelSubtasks, int subtaskIndex) {

        this(
                isCheckpointingEnabled,
                numParallelSubtasks,
                subtaskIndex,
                new MockEnvironmentBuilder()
                        .setTaskName("mockTask")
                        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
                        .build());
    }

    public MockStreamingRuntimeContext(
            boolean isCheckpointingEnabled,
            int numParallelSubtasks,
            int subtaskIndex,
            MockEnvironment environment) {

        super(new MockStreamOperator(), environment, new HashMap<>());

        this.isCheckpointingEnabled = isCheckpointingEnabled;
        this.numParallelSubtasks = numParallelSubtasks;
        this.subtaskIndex = subtaskIndex;
    }

    @Override
    public MetricGroup getMetricGroup() {
        return new UnregisteredMetricsGroup();
    }

    @Override
    public boolean isCheckpointingEnabled() {
        return isCheckpointingEnabled;
    }

    @Override
    public int getIndexOfThisSubtask() {
        return subtaskIndex;
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return numParallelSubtasks;
    }

    private static class MockStreamOperator extends AbstractStreamOperator<Integer> {
        private static final long serialVersionUID = -1153976702711944427L;

        private transient TestProcessingTimeService testProcessingTimeService;

        @Override
        public ExecutionConfig getExecutionConfig() {
            return new ExecutionConfig();
        }

        @Override
        public OperatorID getOperatorID() {
            return new OperatorID();
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            if (testProcessingTimeService == null) {
                testProcessingTimeService = new TestProcessingTimeService();
            }
            return testProcessingTimeService;
        }
    }
}
