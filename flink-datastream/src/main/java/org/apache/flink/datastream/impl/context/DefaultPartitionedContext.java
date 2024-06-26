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

package org.apache.flink.datastream.impl.context;

import org.apache.flink.datastream.api.context.EventTimeManager;
import org.apache.flink.datastream.api.context.JobInfo;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.context.ProcessingTimeManager;
import org.apache.flink.datastream.api.context.RuntimeContext;
import org.apache.flink.datastream.api.context.TaskInfo;
import org.apache.flink.metrics.MetricGroup;

import java.util.function.Consumer;
import java.util.function.Supplier;

/** The default implementation of {@link PartitionedContext}. */
public class DefaultPartitionedContext implements PartitionedContext {
    private final RuntimeContext context;

    private final DefaultStateManager stateManager;

    private final ProcessingTimeManager processingTimeManager;

    private final EventTimeManager eventTimeManager;

    public DefaultPartitionedContext(
            RuntimeContext context,
            Supplier<Object> currentKeySupplier,
            Consumer<Object> currentKeySetter,
            ProcessingTimeManager processingTimeManager,
            EventTimeManager eventTimeManager) {
        this.context = context;
        this.stateManager = new DefaultStateManager(currentKeySupplier, currentKeySetter);
        this.processingTimeManager = processingTimeManager;
        this.eventTimeManager = eventTimeManager;
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
    public DefaultStateManager getStateManager() {
        return stateManager;
    }

    @Override
    public ProcessingTimeManager getProcessingTimeManager() {
        return processingTimeManager;
    }

    @Override
    public EventTimeManager getEventTimeManager() {
        return eventTimeManager;
    }

    @Override
    public MetricGroup getMetricGroup() {
        return context.getMetricGroup();
    }
}
