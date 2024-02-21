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

package org.apache.flink.runtime.failure;

import org.apache.flink.api.common.JobInfo;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.failure.FailureEnricher.Context;
import org.apache.flink.metrics.MetricGroup;

import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The default implementation of {@link Context} class. */
public class DefaultFailureEnricherContext implements FailureEnricher.Context {
    private final JobInfo jobInfo;
    private final MetricGroup metricGroup;
    private final Executor ioExecutor;
    private final ClassLoader userClassLoader;
    private final FailureType failureType;

    private DefaultFailureEnricherContext(
            JobInfo jobInfo,
            MetricGroup metricGroup,
            FailureType failureType,
            Executor ioExecutor,
            ClassLoader classLoader) {
        this.jobInfo = jobInfo;
        this.metricGroup = metricGroup;
        this.failureType = failureType;
        this.ioExecutor = checkNotNull(ioExecutor);
        this.userClassLoader = classLoader;
    }

    @Override
    public MetricGroup getMetricGroup() {
        return this.metricGroup;
    }

    @Override
    public FailureType getFailureType() {
        return failureType;
    }

    @Override
    public ClassLoader getUserClassLoader() {
        return this.userClassLoader;
    }

    @Override
    public Executor getIOExecutor() {
        return ioExecutor;
    }

    @Override
    public JobInfo getJobInfo() {
        return jobInfo;
    }

    /** Factory method returning a Task failure Context for the given params. */
    public static Context forTaskFailure(
            JobInfo jobInfo,
            MetricGroup metricGroup,
            Executor ioExecutor,
            ClassLoader classLoader) {
        return new DefaultFailureEnricherContext(
                jobInfo, metricGroup, FailureType.TASK, ioExecutor, classLoader);
    }

    /** Factory method returning a Global failure Context for the given params. */
    public static Context forGlobalFailure(
            JobInfo jobInfo,
            MetricGroup metricGroup,
            Executor ioExecutor,
            ClassLoader classLoader) {
        return new DefaultFailureEnricherContext(
                jobInfo, metricGroup, FailureType.GLOBAL, ioExecutor, classLoader);
    }

    /** Factory method returning a TaskManager failure Context for the given params. */
    public static Context forTaskManagerFailure(
            JobInfo jobInfo,
            MetricGroup metricGroup,
            Executor ioExecutor,
            ClassLoader classLoader) {
        return new DefaultFailureEnricherContext(
                jobInfo, metricGroup, FailureType.TASK_MANAGER, ioExecutor, classLoader);
    }
}
