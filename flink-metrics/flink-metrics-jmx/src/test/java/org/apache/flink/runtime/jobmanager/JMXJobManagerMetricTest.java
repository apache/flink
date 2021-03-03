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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.jmx.JMXReporter;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/** Tests to verify JMX reporter functionality on the JobManager. */
public class JMXJobManagerMetricTest extends TestLogger {

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    private static Configuration getConfiguration() {
        Configuration flinkConfiguration = new Configuration();

        flinkConfiguration.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
                JMXReporter.class.getName());
        flinkConfiguration.setString(MetricOptions.SCOPE_NAMING_JM_JOB, "jobmanager.<job_name>");

        return flinkConfiguration;
    }

    /** Tests that metrics registered on the JobManager are actually accessible via JMX. */
    @Test
    public void testJobManagerJMXMetricAccess() throws Exception {
        Deadline deadline = Deadline.now().plus(Duration.ofMinutes(2));

        try {
            JobVertex sourceJobVertex = new JobVertex("Source");
            sourceJobVertex.setInvokableClass(BlockingInvokable.class);

            final JobCheckpointingSettings jobCheckpointingSettings =
                    new JobCheckpointingSettings(
                            new CheckpointCoordinatorConfiguration(
                                    500,
                                    500,
                                    50,
                                    5,
                                    CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
                                    true,
                                    false,
                                    false,
                                    0),
                            null);

            final JobGraph jobGraph =
                    JobGraphBuilder.newStreamingJobGraphBuilder()
                            .setJobName("TestingJob")
                            .addJobVertex(sourceJobVertex)
                            .setJobCheckpointingSettings(jobCheckpointingSettings)
                            .build();

            ClusterClient<?> client = MINI_CLUSTER_RESOURCE.getClusterClient();
            client.submitJob(jobGraph).get();

            FutureUtils.retrySuccessfulWithDelay(
                            () -> client.getJobStatus(jobGraph.getJobID()),
                            Time.milliseconds(10),
                            deadline,
                            status -> status == JobStatus.RUNNING,
                            TestingUtils.defaultScheduledExecutor())
                    .get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            Set<ObjectName> nameSet =
                    mBeanServer.queryNames(
                            new ObjectName(
                                    "org.apache.flink.jobmanager.job.lastCheckpointSize:job_name=TestingJob,*"),
                            null);
            Assert.assertEquals(1, nameSet.size());
            assertEquals(-1L, mBeanServer.getAttribute(nameSet.iterator().next(), "Value"));

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
