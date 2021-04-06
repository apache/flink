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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationRejection;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.rpc.TestingRpcServiceResource;
import org.apache.flink.runtime.taskmanager.LocalUnresolvedTaskManagerLocation;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Tests for the {@link DefaultJobLeaderService}. */
public class DefaultJobLeaderServiceTest extends TestLogger {

    @Rule
    public final TestingRpcServiceResource rpcServiceResource = new TestingRpcServiceResource();

    /**
     * Tests that we can concurrently modify the JobLeaderService and complete the leader retrieval
     * operation. See FLINK-16373.
     */
    @Test
    public void handlesConcurrentJobAdditionsAndLeaderChanges() throws Exception {
        final JobLeaderService jobLeaderService =
                new DefaultJobLeaderService(
                        new LocalUnresolvedTaskManagerLocation(),
                        RetryingRegistrationConfiguration.defaultConfiguration());

        final TestingJobLeaderListener jobLeaderListener = new TestingJobLeaderListener();
        final int numberOperations = 20;
        final BlockingQueue<SettableLeaderRetrievalService> instantiatedLeaderRetrievalServices =
                new ArrayBlockingQueue<>(numberOperations);

        final HighAvailabilityServices haServices =
                new TestingHighAvailabilityServicesBuilder()
                        .setJobMasterLeaderRetrieverFunction(
                                leaderForJobId -> {
                                    final SettableLeaderRetrievalService leaderRetrievalService =
                                            new SettableLeaderRetrievalService();
                                    instantiatedLeaderRetrievalServices.offer(
                                            leaderRetrievalService);
                                    return leaderRetrievalService;
                                })
                        .build();

        jobLeaderService.start(
                "foobar", rpcServiceResource.getTestingRpcService(), haServices, jobLeaderListener);

        final CheckedThread addJobAction =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        for (int i = 0; i < numberOperations; i++) {
                            final JobID jobId = JobID.generate();
                            jobLeaderService.addJob(jobId, "foobar");
                            Thread.yield();
                            jobLeaderService.removeJob(jobId);
                        }
                    }
                };
        addJobAction.start();

        for (int i = 0; i < numberOperations; i++) {
            final SettableLeaderRetrievalService leaderRetrievalService =
                    instantiatedLeaderRetrievalServices.take();
            leaderRetrievalService.notifyListener("foobar", UUID.randomUUID());
        }

        addJobAction.sync();
    }

    /**
     * Tests that the JobLeaderService won't try to reconnect to JobMaster after it has lost the
     * leadership. See FLINK-16836.
     */
    @Test
    public void doesNotReconnectAfterTargetLostLeadership() throws Exception {
        final JobID jobId = new JobID();

        final SettableLeaderRetrievalService leaderRetrievalService =
                new SettableLeaderRetrievalService();
        final TestingHighAvailabilityServices haServices =
                new TestingHighAvailabilityServicesBuilder()
                        .setJobMasterLeaderRetrieverFunction(ignored -> leaderRetrievalService)
                        .build();
        final TestingJobMasterGateway jobMasterGateway = registerJobMaster();

        final OneShotLatch jobManagerGainedLeadership = new OneShotLatch();
        final TestingJobLeaderListener testingJobLeaderListener =
                new TestingJobLeaderListener(ignored -> jobManagerGainedLeadership.trigger());

        final JobLeaderService jobLeaderService =
                createAndStartJobLeaderService(haServices, testingJobLeaderListener);

        try {
            jobLeaderService.addJob(jobId, jobMasterGateway.getAddress());

            leaderRetrievalService.notifyListener(jobMasterGateway.getAddress(), UUID.randomUUID());

            jobManagerGainedLeadership.await();

            // revoke the leadership
            leaderRetrievalService.notifyListener(null, null);
            testingJobLeaderListener.waitUntilJobManagerLostLeadership();

            jobLeaderService.reconnect(jobId);
        } finally {
            jobLeaderService.stop();
        }
    }

    /**
     * Tests that the JobLeaderService can reconnect to an old leader which seemed to have lost the
     * leadership in between. See FLINK-14316.
     */
    @Test
    public void canReconnectToOldLeaderWithSameLeaderAddress() throws Exception {
        final JobID jobId = new JobID();

        final SettableLeaderRetrievalService leaderRetrievalService =
                new SettableLeaderRetrievalService();
        final TestingHighAvailabilityServices haServices =
                new TestingHighAvailabilityServicesBuilder()
                        .setJobMasterLeaderRetrieverFunction(ignored -> leaderRetrievalService)
                        .build();

        final TestingJobMasterGateway jobMasterGateway = registerJobMaster();

        final BlockingQueue<JobID> leadershipQueue = new ArrayBlockingQueue<>(1);
        final TestingJobLeaderListener testingJobLeaderListener =
                new TestingJobLeaderListener(leadershipQueue::offer);

        final JobLeaderService jobLeaderService =
                createAndStartJobLeaderService(haServices, testingJobLeaderListener);

        try {
            jobLeaderService.addJob(jobId, jobMasterGateway.getAddress());

            final UUID leaderSessionId = UUID.randomUUID();
            leaderRetrievalService.notifyListener(jobMasterGateway.getAddress(), leaderSessionId);

            // wait for the first leadership
            assertThat(leadershipQueue.take(), is(jobId));

            // revoke the leadership
            leaderRetrievalService.notifyListener(null, null);

            testingJobLeaderListener.waitUntilJobManagerLostLeadership();

            leaderRetrievalService.notifyListener(jobMasterGateway.getAddress(), leaderSessionId);

            // check that we obtain the leadership a second time
            assertThat(leadershipQueue.take(), is(jobId));
        } finally {
            jobLeaderService.stop();
        }
    }

    @Test
    public void
            removeJobWithFailingLeaderRetrievalServiceStopWillStopListeningToLeaderNotifications()
                    throws Exception {
        final FailingSettableLeaderRetrievalService leaderRetrievalService =
                new FailingSettableLeaderRetrievalService();
        final TestingHighAvailabilityServices haServices =
                new TestingHighAvailabilityServicesBuilder()
                        .setJobMasterLeaderRetrieverFunction(ignored -> leaderRetrievalService)
                        .build();

        final JobID jobId = new JobID();
        final CompletableFuture<JobID> newLeaderFuture = new CompletableFuture<>();
        final TestingJobLeaderListener testingJobLeaderListener =
                new TestingJobLeaderListener(newLeaderFuture::complete);

        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder().build();
        rpcServiceResource
                .getTestingRpcService()
                .registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

        final JobLeaderService jobLeaderService =
                createAndStartJobLeaderService(haServices, testingJobLeaderListener);

        try {
            jobLeaderService.addJob(jobId, "foobar");

            jobLeaderService.removeJob(jobId);

            leaderRetrievalService.notifyListener(
                    jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());

            try {
                newLeaderFuture.get(10, TimeUnit.MILLISECONDS);
                fail("The leader future should not be completed.");
            } catch (TimeoutException expected) {
            }
        } finally {
            jobLeaderService.stop();
        }
    }

    @Test
    public void rejectedJobManagerRegistrationCallsJobLeaderListener() throws Exception {
        final SettableLeaderRetrievalService leaderRetrievalService =
                new SettableLeaderRetrievalService();
        final TestingHighAvailabilityServices haServices =
                new TestingHighAvailabilityServicesBuilder()
                        .setJobMasterLeaderRetrieverFunction(ignored -> leaderRetrievalService)
                        .build();

        final JobID jobId = new JobID();
        final CompletableFuture<JobID> rejectedRegistrationFuture = new CompletableFuture<>();
        final TestingJobLeaderListener testingJobLeaderListener =
                new TestingJobLeaderListener(ignored -> {}, rejectedRegistrationFuture::complete);

        final JobLeaderService jobLeaderService =
                createAndStartJobLeaderService(haServices, testingJobLeaderListener);

        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setRegisterTaskManagerFunction(
                                (s, unresolvedTaskManagerLocation, jobID) ->
                                        CompletableFuture.completedFuture(
                                                new JMTMRegistrationRejection("foobar")))
                        .build();

        rpcServiceResource
                .getTestingRpcService()
                .registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

        try {
            jobLeaderService.addJob(jobId, "foobar");

            leaderRetrievalService.notifyListener(
                    jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());

            assertThat(rejectedRegistrationFuture.get(), is(jobId));
        } finally {
            jobLeaderService.stop();
        }
    }

    private static final class FailingSettableLeaderRetrievalService
            extends SettableLeaderRetrievalService {
        @Override
        public void stop() throws FlinkException {
            throw new FlinkException("Test exception");
        }
    }

    private JobLeaderService createAndStartJobLeaderService(
            HighAvailabilityServices haServices, JobLeaderListener testingJobLeaderListener) {
        final JobLeaderService jobLeaderService =
                new DefaultJobLeaderService(
                        new LocalUnresolvedTaskManagerLocation(),
                        RetryingRegistrationConfiguration.defaultConfiguration());

        jobLeaderService.start(
                "foobar",
                rpcServiceResource.getTestingRpcService(),
                haServices,
                testingJobLeaderListener);
        return jobLeaderService;
    }

    private TestingJobMasterGateway registerJobMaster() {
        final TestingJobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder().build();
        rpcServiceResource
                .getTestingRpcService()
                .registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

        return jobMasterGateway;
    }

    private static final class TestingJobLeaderListener implements JobLeaderListener {

        private final CountDownLatch jobManagerLostLeadership = new CountDownLatch(1);

        private final Consumer<JobID> jobManagerGainedLeadership;

        private final Consumer<JobID> jobManagerRejectedRegistrationConsumer;

        private TestingJobLeaderListener() {
            this(ignored -> {});
        }

        private TestingJobLeaderListener(Consumer<JobID> jobManagerGainedLeadership) {
            this(jobManagerGainedLeadership, ignored -> {});
        }

        private TestingJobLeaderListener(
                Consumer<JobID> jobManagerGainedLeadership,
                Consumer<JobID> jobManagerRejectedRegistrationConsumer) {
            this.jobManagerGainedLeadership = jobManagerGainedLeadership;
            this.jobManagerRejectedRegistrationConsumer = jobManagerRejectedRegistrationConsumer;
        }

        @Override
        public void jobManagerGainedLeadership(
                JobID jobId,
                JobMasterGateway jobManagerGateway,
                JMTMRegistrationSuccess registrationMessage) {
            jobManagerGainedLeadership.accept(jobId);
        }

        @Override
        public void jobManagerLostLeadership(JobID jobId, JobMasterId jobMasterId) {
            jobManagerLostLeadership.countDown();
        }

        @Override
        public void handleError(Throwable throwable) {
            // ignored
        }

        @Override
        public void jobManagerRejectedRegistration(
                JobID jobId, String targetAddress, JMTMRegistrationRejection rejection) {
            jobManagerRejectedRegistrationConsumer.accept(jobId);
        }

        private void waitUntilJobManagerLostLeadership() throws InterruptedException {
            jobManagerLostLeadership.await();
        }
    }
}
