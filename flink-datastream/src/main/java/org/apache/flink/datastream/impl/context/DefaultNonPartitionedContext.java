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

import org.apache.flink.api.watermark.WatermarkManager;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.JobInfo;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.TaskInfo;
import org.apache.flink.datastream.api.function.ApplyPartitionFunction;
import org.apache.flink.datastream.impl.watermark.DefaultWatermarkManager;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.Output;

import java.util.Set;

/** The default implementation of {@link NonPartitionedContext}. */
public class DefaultNonPartitionedContext<OUT> implements NonPartitionedContext<OUT> {
    private final DefaultRuntimeContext context;

    private final DefaultPartitionedContext partitionedContext;

    private final Collector<OUT> collector;

    private final boolean isKeyed;

    private final Set<Object> keySet;

    private final WatermarkManager watermarkManager;

    public DefaultNonPartitionedContext(
            DefaultRuntimeContext context,
            DefaultPartitionedContext partitionedContext,
            Collector<OUT> collector,
            boolean isKeyed,
            Set<Object> keySet,
            Output<?> streamRecordOutput) {
        this.context = context;
        this.partitionedContext = partitionedContext;
        this.collector = collector;
        this.isKeyed = isKeyed;
        this.keySet = keySet;
        this.watermarkManager = new DefaultWatermarkManager(streamRecordOutput);
    }

    @Override
    public void applyToAllPartitions(ApplyPartitionFunction<OUT> applyPartitionFunction)
            throws Exception {
        if (isKeyed) {
            for (Object key : keySet) {
                partitionedContext
                        .getStateManager()
                        .executeInKeyContext(
                                () -> {
                                    try {
                                        applyPartitionFunction.apply(collector, partitionedContext);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                },
                                key);
            }
        } else {
            // non-keyed operator has only one partition.
            applyPartitionFunction.apply(collector, partitionedContext);
        }
    }

    @Override
    public WatermarkManager getWatermarkManager() {
        return watermarkManager;
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
    public MetricGroup getMetricGroup() {
        return context.getMetricGroup();
    }
}
