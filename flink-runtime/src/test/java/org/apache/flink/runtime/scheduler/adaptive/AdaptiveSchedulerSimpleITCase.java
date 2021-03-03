/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/** Integration tests for the adaptive scheduler. */
public class AdaptiveSchedulerSimpleITCase extends TestLogger {

    private static final int NUMBER_TASK_MANAGERS = 2;
    private static final int NUMBER_SLOTS_PER_TASK_MANAGER = 2;
    private static final int PARALLELISM = 10;

    private static final Configuration configuration = getConfiguration();

    private static Configuration getConfiguration() {
        final Configuration configuration = new Configuration();

        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        configuration.set(ClusterOptions.ENABLE_DECLARATIVE_RESOURCE_MANAGEMENT, true);

        return configuration;
    }

    @ClassRule
    public static final MiniClusterResource MINI_CLUSTER_RESOURCE =
            new MiniClusterResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(configuration)
                            .setNumberTaskManagers(NUMBER_TASK_MANAGERS)
                            .setNumberSlotsPerTaskManager(NUMBER_SLOTS_PER_TASK_MANAGER)
                            .build());

    @Test
    public void testSchedulingOfSimpleJob() throws Exception {
        assumeTrue(ClusterOptions.isDeclarativeResourceManagementEnabled(configuration));

        final MiniCluster miniCluster = MINI_CLUSTER_RESOURCE.getMiniCluster();
        final JobGraph jobGraph = createJobGraph();

        miniCluster.submitJob(jobGraph).join();

        final JobResult jobResult = miniCluster.requestJobResult(jobGraph.getJobID()).join();

        final JobExecutionResult jobExecutionResult =
                jobResult.toJobExecutionResult(getClass().getClassLoader());

        assertTrue(jobResult.isSuccess());
    }

    private JobGraph createJobGraph() {
        final JobVertex source = new JobVertex("Source");
        source.setInvokableClass(NoOpInvokable.class);
        source.setParallelism(PARALLELISM);

        final JobVertex sink = new JobVertex("sink");
        sink.setInvokableClass(NoOpInvokable.class);
        sink.setParallelism(PARALLELISM);

        sink.connectNewDataSetAsInput(
                source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        return JobGraphTestUtils.streamingJobGraph(source, sink);
    }

    @Test
    public void testGlobalFailoverIfTaskFails() throws Throwable {
        assumeTrue(ClusterOptions.isDeclarativeResourceManagementEnabled(configuration));

        final MiniCluster miniCluster = MINI_CLUSTER_RESOURCE.getMiniCluster();
        final JobGraph jobGraph = createOnceFailingJobGraph();

        miniCluster.submitJob(jobGraph).join();

        final JobResult jobResult = miniCluster.requestJobResult(jobGraph.getJobID()).join();

        if (!jobResult.isSuccess()) {
            throw jobResult
                    .getSerializedThrowable()
                    .get()
                    .deserializeError(ClassLoader.getSystemClassLoader());
        }
    }

    private JobGraph createOnceFailingJobGraph() throws IOException {
        final JobVertex onceFailingOperator = new JobVertex("Once failing operator");

        OnceFailingInvokable.reset();
        onceFailingOperator.setInvokableClass(OnceFailingInvokable.class);

        onceFailingOperator.setParallelism(1);
        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(onceFailingOperator);
        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));
        jobGraph.setExecutionConfig(executionConfig);
        return jobGraph;
    }

    /** Once failing {@link AbstractInvokable}. */
    public static final class OnceFailingInvokable extends AbstractInvokable {
        private static volatile boolean hasFailed = false;

        /**
         * Create an Invokable task and set its environment.
         *
         * @param environment The environment assigned to this invokable.
         */
        public OnceFailingInvokable(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            if (!hasFailed && getIndexInSubtaskGroup() == 0) {
                hasFailed = true;
                throw new FlinkRuntimeException("Test failure.");
            }
        }

        private static void reset() {
            hasFailed = false;
        }
    }
}
