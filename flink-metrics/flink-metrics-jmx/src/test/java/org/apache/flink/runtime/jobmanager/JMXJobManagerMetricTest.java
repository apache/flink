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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.jmx.JMXReporterFactory;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests to verify JMX reporter functionality on the JobManager. */
class JMXJobManagerMetricTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    private static Configuration getConfiguration() {
        Configuration flinkConfiguration = new Configuration();

        MetricOptions.forReporter(flinkConfiguration, "test")
                .set(MetricOptions.REPORTER_FACTORY_CLASS, JMXReporterFactory.class.getName());
        flinkConfiguration.setString(MetricOptions.SCOPE_NAMING_JM_JOB, "jobmanager.<job_name>");

        return flinkConfiguration;
    }

    /** Tests that metrics registered on the JobManager are actually accessible via JMX. */
    @Test
    void testJobManagerJMXMetricAccess(@InjectClusterClient ClusterClient<?> client)
            throws Exception {
        Deadline deadline = Deadline.now().plus(Duration.ofMinutes(2));

        try {
            JobVertex sourceJobVertex = new JobVertex("Source");
            sourceJobVertex.setInvokableClass(BlockingInvokable.class);
            sourceJobVertex.setParallelism(1);

            final JobCheckpointingSettings jobCheckpointingSettings =
                    new JobCheckpointingSettings(
                            CheckpointCoordinatorConfiguration.builder().build(), null);

            final JobGraph jobGraph =
                    JobGraphBuilder.newStreamingJobGraphBuilder()
                            .setJobName("TestingJob")
                            .addJobVertex(sourceJobVertex)
                            .setJobCheckpointingSettings(jobCheckpointingSettings)
                            .build();

            client.submitJob(jobGraph).get();

            FutureUtils.retrySuccessfulWithDelay(
                            () -> client.getJobStatus(jobGraph.getJobID()),
                            Duration.ofMillis(10),
                            deadline,
                            status -> status == JobStatus.RUNNING,
                            new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()))
                    .get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            Set<ObjectName> nameSet =
                    mBeanServer.queryNames(
                            new ObjectName(
                                    "org.apache.flink.jobmanager.job.lastCheckpointSize:job_name=TestingJob,*"),
                            null);
            assertThat(nameSet).hasSize(1);
            assertThat(mBeanServer.getAttribute(nameSet.iterator().next(), "Value")).isEqualTo(-1L);

            BlockingInvokable.unblock();
        } finally {
            BlockingInvokable.unblock();
        }
    }

    /** Utility to block/unblock a task. */
    public static class BlockingInvokable extends AbstractInvokable {

        private static final OneShotLatch LATCH = new OneShotLatch();

        public BlockingInvokable(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            LATCH.await();
        }

        public static void unblock() {
            LATCH.trigger();
        }
    }
}
