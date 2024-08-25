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

import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.JobInfo;
import org.apache.flink.datastream.api.context.TaskInfo;
import org.apache.flink.datastream.api.context.TwoOutputNonPartitionedContext;
import org.apache.flink.datastream.api.function.TwoOutputApplyPartitionFunction;
import org.apache.flink.metrics.MetricGroup;

import java.util.Set;

/** The default implementation of {@link TwoOutputNonPartitionedContext}. */
public class DefaultTwoOutputNonPartitionedContext<OUT1, OUT2>
        implements TwoOutputNonPartitionedContext<OUT1, OUT2> {
    protected final DefaultRuntimeContext context;

    private final DefaultAbstractPartitionedContext partitionedContext;

    protected final Collector<OUT1> firstCollector;

    protected final Collector<OUT2> secondCollector;

    private final boolean isKeyed;

    private final Set<Object> keySet;

    public DefaultTwoOutputNonPartitionedContext(
            DefaultRuntimeContext context,
            DefaultAbstractPartitionedContext partitionedContext,
            Collector<OUT1> firstCollector,
            Collector<OUT2> secondCollector,
            boolean isKeyed,
            Set<Object> keySet) {
        this.context = context;
        this.partitionedContext = partitionedContext;
        this.firstCollector = firstCollector;
        this.secondCollector = secondCollector;
        this.isKeyed = isKeyed;
        this.keySet = keySet;
    }

    @Override
    public void applyToAllPartitions(
            TwoOutputApplyPartitionFunction<OUT1, OUT2> applyPartitionFunction) throws Exception {
        if (isKeyed) {
            for (Object key : keySet) {
                partitionedContext
                        .getStateManager()
                        .executeInKeyContext(
                                () -> {
                                    try {
                                        applyPartitionFunction.apply(
                                                firstCollector,
                                                secondCollector,
                                                partitionedContext);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                },
                                key);
            }
        } else {
            // non-keyed operator has only one partition.
            applyPartitionFunction.apply(firstCollector, secondCollector, partitionedContext);
        }
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
