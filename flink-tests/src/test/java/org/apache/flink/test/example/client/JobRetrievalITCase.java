/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.example.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.concurrent.Semaphore;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests retrieval of a job from a running Flink cluster. */
@ExtendWith(TestLoggerExtension.class)
class JobRetrievalITCase {

    private static final Semaphore lock = new Semaphore(1);

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    private RestClusterClient<StandaloneClusterId> client;

    @BeforeEach
    void setUp(@InjectClusterClient RestClusterClient<?> restClusterClient) throws Exception {
        final Configuration clientConfig = new Configuration();
        clientConfig.addAll(restClusterClient.getFlinkConfiguration());
        clientConfig.set(RestOptions.RETRY_MAX_ATTEMPTS, 0);
        clientConfig.set(RestOptions.RETRY_DELAY, Duration.ofMillis(0L));

        client = new RestClusterClient<>(clientConfig, StandaloneClusterId.getInstance());
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void testJobRetrieval() throws Exception {
        final JobVertex imalock = new JobVertex("imalock");
        imalock.setInvokableClass(SemaphoreInvokable.class);
        imalock.setParallelism(1);

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(imalock);
        final JobID jobId = jobGraph.getJobID();

        // acquire the lock to make sure that the job cannot complete until the job client
        // has been attached in resumingThread
        lock.acquire();

        client.submitJob(jobGraph).get();

        final CheckedThread resumingThread =
                new CheckedThread("Flink-Job-Retriever") {
                    @Override
                    public void go() throws Exception {
                        assertThat(client.requestJobResult(jobId).get()).isNotNull();
                    }
                };

        // wait until the job is running
        waitUntilCondition(() -> !client.listJobs().get().isEmpty(), 20);

        // kick off resuming
        resumingThread.start();

        // wait for client to connect
        waitUntilCondition(() -> resumingThread.getState() == Thread.State.WAITING, 20);

        // client has connected, we can release the lock
        lock.release();

        resumingThread.sync();
    }

    @Test
    void testNonExistingJobRetrieval() {
        final JobID jobID = new JobID();

        assertThatThrownBy(() -> client.requestJobResult(jobID).get())
                .hasMessageContaining("Could not find Flink job");
    }

    /**
     * Invokable that waits on {@link #lock} to be released and finishes afterwards.
     *
     * <p>NOTE: needs to be <tt>public</tt> so that a task can be run with this!
     */
    public static class SemaphoreInvokable extends AbstractInvokable {

        public SemaphoreInvokable(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            lock.acquire();
            lock.release();
        }
    }
}
