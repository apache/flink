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

package org.apache.flink.runtime.executiongraph.utils;

import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import java.util.Set;

/**
 * A {@code FailoverStrategyFactory} provides a {@link FailoverStrategy} implementation that can be
 * used in tests to specify the vertices that need to be restarted.
 */
public class TestFailoverStrategyFactory implements FailoverStrategy.Factory {

    private Set<ExecutionVertexID> tasksToRestart;

    public TestFailoverStrategyFactory() {}

    public void setTasksToRestart(ExecutionVertexID... tasksToRestart) {
        this.tasksToRestart = Sets.newHashSet(tasksToRestart);
    }

    @Override
    public FailoverStrategy create(
            SchedulingTopology topology,
            ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker) {
        return new TestFailoverStrategy();
    }

    private class TestFailoverStrategy implements FailoverStrategy {

        @Override
        public Set<ExecutionVertexID> getTasksNeedingRestart(
                ExecutionVertexID executionVertexId, Throwable cause) {
            return TestFailoverStrategyFactory.this.tasksToRestart;
        }
    }
}
