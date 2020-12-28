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
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/** Tests retrieval of a job from a running Flink cluster. */
public class JobRetrievalITCase extends TestLogger {

    private static final Semaphore lock = new Semaphore(1);

    @ClassRule
    public static final MiniClusterResource CLUSTER =
            new MiniClusterResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    private RestClusterClient<StandaloneClusterId> client;

    @Before
    public void setUp() throws Exception {
        final Configuration clientConfig = new Configuration();
        clientConfig.setInteger(RestOptions.RETRY_MAX_ATTEMPTS, 0);
        clientConfig.setLong(RestOptions.RETRY_DELAY, 0);
        clientConfig.addAll(CLUSTER.getClientConfiguration());

        client = new RestClusterClient<>(clientConfig, StandaloneClusterId.getInstance());
    }

    @After
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void testJobRetrieval() throws Exception {
        final JobID jobID = new JobID();

        final JobVertex imalock = new JobVertex("imalock");
        imalock.setInvokableClass(SemaphoreInvokable.class);

        final JobGraph jobGraph = new JobGraph(jobID, "testjob", imalock);

        // acquire the lock to make sure that the job cannot complete until the job client
        // has been attached in resumingThread
        lock.acquire();

        client.submitJob(jobGraph).get();

        final CheckedThread resumingThread =
                new CheckedThread("Flink-Job-Retriever") {
                    @Override
                    public void go() throws Exception {
                        assertNotNull(client.requestJobResult(jobID).get());
                    }
                };

        // wait until the job is running
        while (client.listJobs().get().isEmpty()) {
            Thread.sleep(50);
        }

        // kick off resuming
        resumingThread.start();

        // wait for client to connect
        while (resumingThread.getState() != Thread.State.WAITING) {
            Thread.sleep(10);
        }

        // client has connected, we can release the lock
        lock.release();

        resumingThread.sync();
    }

    @Test
    public void testNonExistingJobRetrieval() throws Exception {
        final JobID jobID = new JobID();

        try {
            client.requestJobResult(jobID).get();
            fail();
        } catch (Exception exception) {
            Optional<Throwable> expectedCause =
                    ExceptionUtils.findThrowable(
                            exception,
                            candidate ->
                                    candidate.getMessage() != null
                                            && candidate
                                                    .getMessage()
                                                    .contains("Could not find Flink job"));
            if (!expectedCause.isPresent()) {
                throw exception;
            }
        }
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
