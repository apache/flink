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

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DefaultDispatcherRunner}. */
class DefaultDispatcherRunnerTest {

    private TestingLeaderElection leaderElection;
    private TestingFatalErrorHandler testingFatalErrorHandler;
    private TestingDispatcherLeaderProcessFactory testingDispatcherLeaderProcessFactory;

    @BeforeEach
    void setup() {
        leaderElection = new TestingLeaderElection();
        testingFatalErrorHandler = new TestingFatalErrorHandler();
        testingDispatcherLeaderProcessFactory =
                TestingDispatcherLeaderProcessFactory.defaultValue();
    }

    @AfterEach
    void teardown() throws Exception {
        leaderElection.close();

        if (testingFatalErrorHandler != null) {
            testingFatalErrorHandler.rethrowError();
            testingFatalErrorHandler = null;
        }
    }

    @Test
    void testLeaderElectionLifecycle() throws Exception {
        assertThat(leaderElection.isStopped()).isTrue();
        try (final DispatcherRunner unusedDisptacherRunner = createDispatcherRunner()) {
            assertThat(leaderElection.isStopped()).isFalse();
        }
        assertThat(leaderElection.isStopped()).isTrue();
    }

    @Test
    void closeAsync_doesNotCompleteUncompletedShutDownFuture() throws Exception {
        final DispatcherRunner dispatcherRunner = createDispatcherRunner();

        final CompletableFuture<Void> terminationFuture = dispatcherRunner.closeAsync();
        terminationFuture.get();

        final CompletableFuture<ApplicationStatus> shutDownFuture =
                dispatcherRunner.getShutDownFuture();
        assertThat(shutDownFuture).isNotDone();
    }

    @Test
    void getShutDownFuture_whileRunning_forwardsDispatcherLeaderProcessShutDownRequest()
            throws Exception {
        final UUID leaderSessionId = UUID.randomUUID();
        final CompletableFuture<ApplicationStatus> shutDownFuture = new CompletableFuture<>();
        final TestingDispatcherLeaderProcess testingDispatcherLeaderProcess =
                TestingDispatcherLeaderProcess.newBuilder(leaderSessionId)
                        .setShutDownFuture(shutDownFuture)
                        .build();
        testingDispatcherLeaderProcessFactory =
                TestingDispatcherLeaderProcessFactory.from(testingDispatcherLeaderProcess);

        try (final DispatcherRunner dispatcherRunner = createDispatcherRunner()) {
            leaderElection.isLeader(leaderSessionId);

            final CompletableFuture<ApplicationStatus> dispatcherShutDownFuture =
                    dispatcherRunner.getShutDownFuture();

            assertThat(dispatcherShutDownFuture).isNotDone();

            final ApplicationStatus finalApplicationStatus = ApplicationStatus.UNKNOWN;
            shutDownFuture.complete(finalApplicationStatus);

            assertThat(dispatcherShutDownFuture.get()).isEqualTo(finalApplicationStatus);
        }
    }

    @Test
    void getShutDownFuture_afterClose_ignoresDispatcherLeaderProcessShutDownRequest()
            throws Exception {
        final UUID leaderSessionId = UUID.randomUUID();
        final CompletableFuture<ApplicationStatus> shutDownFuture = new CompletableFuture<>();
        final TestingDispatcherLeaderProcess testingDispatcherLeaderProcess =
                TestingDispatcherLeaderProcess.newBuilder(leaderSessionId)
                        .setShutDownFuture(shutDownFuture)
                        .build();
        testingDispatcherLeaderProcessFactory =
                TestingDispatcherLeaderProcessFactory.from(testingDispatcherLeaderProcess);

        try (final DispatcherRunner dispatcherRunner = createDispatcherRunner()) {
            leaderElection.isLeader(leaderSessionId);

            final CompletableFuture<ApplicationStatus> dispatcherShutDownFuture =
                    dispatcherRunner.getShutDownFuture();

            assertThat(dispatcherShutDownFuture).isNotDone();

            dispatcherRunner.closeAsync();

            final ApplicationStatus finalApplicationStatus = ApplicationStatus.UNKNOWN;
            shutDownFuture.complete(finalApplicationStatus);

            assertThatFuture(dispatcherShutDownFuture)
                    .as(
                            "The dispatcher runner should no longer react to the dispatcher leader process's shut down request if it has been terminated.")
                    .willNotCompleteWithin(Duration.ofMillis(10L));
        }
    }

    @Test
    void getShutDownFuture_newLeader_ignoresOldDispatcherLeaderProcessShutDownRequest()
            throws Exception {
        final UUID firstLeaderSessionId = UUID.randomUUID();
        final UUID secondLeaderSessionId = UUID.randomUUID();
        final CompletableFuture<ApplicationStatus> shutDownFuture = new CompletableFuture<>();
        final TestingDispatcherLeaderProcess firstTestingDispatcherLeaderProcess =
                TestingDispatcherLeaderProcess.newBuilder(firstLeaderSessionId)
                        .setShutDownFuture(shutDownFuture)
                        .build();
        final TestingDispatcherLeaderProcess secondTestingDispatcherLeaderProcess =
                TestingDispatcherLeaderProcess.newBuilder(secondLeaderSessionId).build();
        testingDispatcherLeaderProcessFactory =
                TestingDispatcherLeaderProcessFactory.from(
                        firstTestingDispatcherLeaderProcess, secondTestingDispatcherLeaderProcess);

        try (final DispatcherRunner dispatcherRunner = createDispatcherRunner()) {
            leaderElection.isLeader(firstLeaderSessionId);

            final CompletableFuture<ApplicationStatus> dispatcherShutDownFuture =
                    dispatcherRunner.getShutDownFuture();

            assertThat(dispatcherShutDownFuture).isNotDone();

            leaderElection.isLeader(secondLeaderSessionId);

            final ApplicationStatus finalApplicationStatus = ApplicationStatus.UNKNOWN;
            shutDownFuture.complete(finalApplicationStatus);

            assertThat(dispatcherShutDownFuture).isNotDone();
        }
    }

    @Test
    void revokeLeadership_withExistingLeader_stopsLeaderProcess() throws Exception {
        final UUID leaderSessionId = UUID.randomUUID();

        final OneShotLatch startLatch = new OneShotLatch();
        final OneShotLatch stopLatch = new OneShotLatch();
        testingDispatcherLeaderProcessFactory =
                TestingDispatcherLeaderProcessFactory.from(
                        TestingDispatcherLeaderProcess.newBuilder(leaderSessionId)
                                .setStartConsumer(ignored -> startLatch.trigger())
                                .setCloseAsyncSupplier(
                                        () -> {
                                            stopLatch.trigger();
                                            return FutureUtils.completedVoidFuture();
                                        })
                                .build());
        try (final DispatcherRunner dispatcherRunner = createDispatcherRunner()) {
            leaderElection.isLeader(leaderSessionId);

            // wait until the leader process has been started
            startLatch.await();

            leaderElection.notLeader();

            // verify that the leader gets stopped
            stopLatch.await();
        }
    }

    @Test
    void grantLeadership_withExistingLeader_waitsForTerminationOfFirstLeader() throws Exception {
        final UUID firstLeaderSessionId = UUID.randomUUID();
        final UUID secondLeaderSessionId = UUID.randomUUID();

        final StartStopDispatcherLeaderProcess firstTestingDispatcherLeaderProcess =
                StartStopDispatcherLeaderProcess.create(firstLeaderSessionId);
        final StartStopDispatcherLeaderProcess secondTestingDispatcherLeaderProcess =
                StartStopDispatcherLeaderProcess.create(secondLeaderSessionId);

        testingDispatcherLeaderProcessFactory =
                TestingDispatcherLeaderProcessFactory.from(
                        firstTestingDispatcherLeaderProcess.asTestingDispatcherLeaderProcess(),
                        secondTestingDispatcherLeaderProcess.asTestingDispatcherLeaderProcess());

        try (final DispatcherRunner dispatcherRunner = createDispatcherRunner()) {
            leaderElection.isLeader(firstLeaderSessionId);

            assertThat(firstTestingDispatcherLeaderProcess.isStarted()).isTrue();

            leaderElection.isLeader(secondLeaderSessionId);

            assertThat(secondTestingDispatcherLeaderProcess.isStarted()).isFalse();
            firstTestingDispatcherLeaderProcess.terminateProcess();
            assertThat(secondTestingDispatcherLeaderProcess.isStarted()).isTrue();
            secondTestingDispatcherLeaderProcess
                    .terminateProcess(); // make the dispatcherRunner terminate
        }
    }

    @Test
    void grantLeadership_validLeader_confirmsLeaderSession() throws Exception {
        final UUID leaderSessionId = UUID.randomUUID();

        try (final DispatcherRunner dispatcherRunner = createDispatcherRunner()) {
            final LeaderInformation leaderInformation =
                    leaderElection.isLeader(leaderSessionId).join();

            assertThat(leaderInformation.getLeaderSessionID()).isEqualTo(leaderSessionId);
        }
    }

    @Test
    void grantLeadership_oldLeader_doesNotConfirmLeaderSession() throws Exception {
        final UUID leaderSessionId = UUID.randomUUID();
        final CompletableFuture<String> contenderConfirmationFuture = new CompletableFuture<>();
        final TestingDispatcherLeaderProcess testingDispatcherLeaderProcess =
                TestingDispatcherLeaderProcess.newBuilder(leaderSessionId)
                        .setConfirmLeaderSessionFuture(contenderConfirmationFuture)
                        .build();

        testingDispatcherLeaderProcessFactory =
                TestingDispatcherLeaderProcessFactory.from(testingDispatcherLeaderProcess);

        try (final DispatcherRunner dispatcherRunner = createDispatcherRunner()) {
            leaderElection.isLeader(leaderSessionId);

            leaderElection.notLeader();

            // complete the confirmation future after losing the leadership
            contenderConfirmationFuture.complete("leader address");

            assertThat(leaderElection.hasLeadershipAsync(leaderSessionId).get()).isFalse();
        }
    }

    @Test
    void
            grantLeadership_multipleLeaderChanges_lastDispatcherLeaderProcessWaitsForOthersToTerminateBeforeItStarts()
                    throws Exception {
        final UUID firstLeaderSession = UUID.randomUUID();
        final UUID secondLeaderSession = UUID.randomUUID();
        final UUID thirdLeaderSession = UUID.randomUUID();

        final CompletableFuture<Void> firstDispatcherLeaderProcessTerminationFuture =
                new CompletableFuture<>();
        final TestingDispatcherLeaderProcess firstDispatcherLeaderProcess =
                TestingDispatcherLeaderProcess.newBuilder(firstLeaderSession)
                        .setCloseAsyncSupplier(() -> firstDispatcherLeaderProcessTerminationFuture)
                        .build();
        final CompletableFuture<Void> secondDispatcherLeaderProcessTerminationFuture =
                new CompletableFuture<>();
        final TestingDispatcherLeaderProcess secondDispatcherLeaderProcess =
                TestingDispatcherLeaderProcess.newBuilder(secondLeaderSession)
                        .setCloseAsyncSupplier(() -> secondDispatcherLeaderProcessTerminationFuture)
                        .build();
        final CompletableFuture<Void> thirdDispatcherLeaderProcessHasBeenStartedFuture =
                new CompletableFuture<>();
        final TestingDispatcherLeaderProcess thirdDispatcherLeaderProcess =
                TestingDispatcherLeaderProcess.newBuilder(thirdLeaderSession)
                        .setStartConsumer(
                                thirdDispatcherLeaderProcessHasBeenStartedFuture::complete)
                        .build();

        testingDispatcherLeaderProcessFactory =
                TestingDispatcherLeaderProcessFactory.from(
                        firstDispatcherLeaderProcess,
                        secondDispatcherLeaderProcess,
                        thirdDispatcherLeaderProcess);

        final DispatcherRunner dispatcherRunner = createDispatcherRunner();

        try {
            leaderElection.isLeader(firstLeaderSession);
            leaderElection.isLeader(secondLeaderSession);
            leaderElection.isLeader(thirdLeaderSession);

            firstDispatcherLeaderProcessTerminationFuture.complete(null);

            assertThat(thirdDispatcherLeaderProcessHasBeenStartedFuture).isNotDone();

            secondDispatcherLeaderProcessTerminationFuture.complete(null);

            assertThat(thirdDispatcherLeaderProcessHasBeenStartedFuture).isDone();
        } finally {
            firstDispatcherLeaderProcessTerminationFuture.complete(null);
            secondDispatcherLeaderProcessTerminationFuture.complete(null);

            dispatcherRunner.close();
        }
    }

    private static final class StartStopDispatcherLeaderProcess {

        private final TestingDispatcherLeaderProcess testingDispatcherLeaderProcess;
        private final CompletableFuture<Void> startFuture;
        private final CompletableFuture<Void> terminationFuture;

        private StartStopDispatcherLeaderProcess(
                TestingDispatcherLeaderProcess testingDispatcherLeaderProcess,
                CompletableFuture<Void> startFuture,
                CompletableFuture<Void> terminationFuture) {
            this.testingDispatcherLeaderProcess = testingDispatcherLeaderProcess;
            this.startFuture = startFuture;
            this.terminationFuture = terminationFuture;
        }

        private TestingDispatcherLeaderProcess asTestingDispatcherLeaderProcess() {
            return testingDispatcherLeaderProcess;
        }

        private boolean isStarted() {
            return startFuture.isDone();
        }

        private void terminateProcess() {
            terminationFuture.complete(null);
        }

        private static StartStopDispatcherLeaderProcess create(UUID leaderSessionId) {
            final CompletableFuture<Void> processStartFuture = new CompletableFuture<>();
            final CompletableFuture<Void> processTerminationFuture = new CompletableFuture<>();
            final TestingDispatcherLeaderProcess dispatcherLeaderProcess =
                    TestingDispatcherLeaderProcess.newBuilder(leaderSessionId)
                            .setStartConsumer(processStartFuture::complete)
                            .setCloseAsyncSupplier(() -> processTerminationFuture)
                            .build();

            return new StartStopDispatcherLeaderProcess(
                    dispatcherLeaderProcess, processStartFuture, processTerminationFuture);
        }
    }

    private DispatcherRunner createDispatcherRunner() throws Exception {
        return DefaultDispatcherRunner.create(
                leaderElection, testingFatalErrorHandler, testingDispatcherLeaderProcessFactory);
    }
}
