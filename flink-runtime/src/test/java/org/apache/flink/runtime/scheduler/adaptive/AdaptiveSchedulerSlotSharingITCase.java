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

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/** SlotSharing tests for the adaptive scheduler. */
public class AdaptiveSchedulerSlotSharingITCase extends TestLogger {

    private static final int NUMBER_TASK_MANAGERS = 1;
    private static final int NUMBER_SLOTS_PER_TASK_MANAGER = 1;
    private static final int PARALLELISM = 10;

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
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(NUMBER_TASK_MANAGERS)
                            .setNumberSlotsPerTaskManager(NUMBER_SLOTS_PER_TASK_MANAGER)
                            .build());

    @Test
    public void testSchedulingOfJobRequiringSlotSharing() throws Exception {
        // run job multiple times to ensure slots are cleaned up properly
        runJob();
        runJob();
    }

    private void runJob() throws Exception {
        final MiniCluster miniCluster = MINI_CLUSTER_RESOURCE.getMiniCluster();
        final JobGraph jobGraph = createJobGraph();

        miniCluster.submitJob(jobGraph).join();

        final JobResult jobResult = miniCluster.requestJobResult(jobGraph.getJobID()).join();

        // this throws an exception if the job failed
        jobResult.toJobExecutionResult(getClass().getClassLoader());

        assertTrue(jobResult.isSuccess());
    }

    /**
     * Returns a JobGraph that requires slot sharing to work in order to be able to run with a
     * single slot.
     */
    private static JobGraph createJobGraph() {
        final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();

        final JobVertex source = new JobVertex("Source");
        source.setInvokableClass(NoOpInvokable.class);
        source.setParallelism(PARALLELISM);
        source.setSlotSharingGroup(slotSharingGroup);

        final JobVertex sink = new JobVertex("sink");
        sink.setInvokableClass(NoOpInvokable.class);
        sink.setParallelism(PARALLELISM);
        sink.setSlotSharingGroup(slotSharingGroup);

        sink.connectNewDataSetAsInput(
                source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        final JobGraph jobGraph = new JobGraph("Simple job", source, sink);
        jobGraph.setJobType(JobType.STREAMING);

        return jobGraph;
    }
}
