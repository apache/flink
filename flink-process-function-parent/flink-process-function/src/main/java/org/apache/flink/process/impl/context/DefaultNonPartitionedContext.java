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

package org.apache.flink.process.impl.context;

import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.process.api.common.Collector;
import org.apache.flink.process.api.context.JobInfo;
import org.apache.flink.process.api.context.NonPartitionedContext;
import org.apache.flink.process.api.context.ProcessingTimeManager;
import org.apache.flink.process.api.context.StateManager;
import org.apache.flink.process.api.context.TaskInfo;
import org.apache.flink.process.api.context.TimestampManager;
import org.apache.flink.process.api.function.ApplyPartitionFunction;

/** The default implementation of {@link NonPartitionedContext}. */
public class DefaultNonPartitionedContext<OUT> implements NonPartitionedContext<OUT> {
    protected final DefaultRuntimeContext context;

    protected final Collector<OUT> collector;

    public DefaultNonPartitionedContext(DefaultRuntimeContext context, Collector<OUT> collector) {
        this.context = context;
        this.collector = collector;
    }

    @Override
    public void applyToAllPartitions(ApplyPartitionFunction<OUT> applyPartitionFunction)
            throws Exception {
        // non-keyed operator has only one partition.
        applyPartitionFunction.apply(collector, context);
    }

    @Override
    public JobInfo getJobInfo() {
        return context.getJobInfo();
    }

    @Override
    public TaskInfo getTaskInfo() {
        return context.getTaskInfo();
    }

    @Override
    public StateManager getStateManager() {
        // state is partition-aware, so it's always empty in non-partitioned context.
        return EmptyStateManager.INSTANCE;
    }

    @Override
    public ProcessingTimeManager getProcessingTimeManager() {
        // processing timer is partition-aware, so it's not supported in non-partitioned context.
        return UnsupportedProcessingTimeManager.INSTANCE;
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return context.getMetricGroup();
    }

    @Override
    public TimestampManager getTimestampManager() {
        return context.getTimestampManager();
    }
}
