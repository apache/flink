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

package org.apache.flink.test.cancelling;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.Plan;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.runtime.testutils.TestingUtils;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.ClassRule;

import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

/** Base class for testing job cancellation. */
public abstract class CancelingTestBase extends TestLogger {

    private static final int MINIMUM_HEAP_SIZE_MB = 192;

    protected static final int PARALLELISM = 4;

    private static final Configuration configuration = getConfiguration();

    // --------------------------------------------------------------------------------------------

    @ClassRule
    public static final MiniClusterWithClientResource CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(configuration)
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    // --------------------------------------------------------------------------------------------

    private static void verifyJvmOptions() {
        final long heap = Runtime.getRuntime().maxMemory() >> 20;
        Assert.assertTrue(
                "Insufficient java heap space "
                        + heap
                        + "mb - set JVM option: -Xmx"
                        + MINIMUM_HEAP_SIZE_MB
                        + "m",
                heap > MINIMUM_HEAP_SIZE_MB - 50);
    }

    private static Configuration getConfiguration() {
        verifyJvmOptions();
        Configuration config = new Configuration();
        config.setBoolean(CoreOptions.FILESYTEM_DEFAULT_OVERRIDE, true);
        config.set(AkkaOptions.ASK_TIMEOUT_DURATION, TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT);
        config.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("4096"));
        config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS, 2048);

        return config;
    }

    // --------------------------------------------------------------------------------------------

    protected void runAndCancelJob(Plan plan, final int msecsTillCanceling, int maxTimeTillCanceled)
            throws Exception {
        // submit job
        final JobGraph jobGraph = getJobGraph(plan);

        final long rpcTimeout = configuration.get(AkkaOptions.ASK_TIMEOUT_DURATION).toMillis();

        ClusterClient<?> client = CLUSTER.getClusterClient();
        JobID jobID = client.submitJob(jobGraph).get();

        Deadline submissionDeadLine = new FiniteDuration(2, TimeUnit.MINUTES).fromNow();

        JobStatus jobStatus = client.getJobStatus(jobID).get(rpcTimeout, TimeUnit.MILLISECONDS);
        while (jobStatus != JobStatus.RUNNING && submissionDeadLine.hasTimeLeft()) {
            Thread.sleep(50);
            jobStatus = client.getJobStatus(jobID).get(rpcTimeout, TimeUnit.MILLISECONDS);
        }
        if (jobStatus != JobStatus.RUNNING) {
            Assert.fail("Job not in state RUNNING.");
        }

        Thread.sleep(msecsTillCanceling);

        client.cancel(jobID).get();

        Deadline cancelDeadline =
                new FiniteDuration(maxTimeTillCanceled, TimeUnit.MILLISECONDS).fromNow();

        JobStatus jobStatusAfterCancel =
                client.getJobStatus(jobID).get(rpcTimeout, TimeUnit.MILLISECONDS);
        while (jobStatusAfterCancel != JobStatus.CANCELED && cancelDeadline.hasTimeLeft()) {
            Thread.sleep(50);
            jobStatusAfterCancel =
                    client.getJobStatus(jobID).get(rpcTimeout, TimeUnit.MILLISECONDS);
        }
        if (jobStatusAfterCancel != JobStatus.CANCELED) {
            Assert.fail("Failed to cancel job with ID " + jobID + '.');
        }
    }

    private JobGraph getJobGraph(final Plan plan) {
        final Optimizer pc = new Optimizer(new DataStatistics(), getConfiguration());
        final OptimizedPlan op = pc.compile(plan);
        final JobGraphGenerator jgg = new JobGraphGenerator();
        return jgg.compileJobGraph(op);
    }
}
