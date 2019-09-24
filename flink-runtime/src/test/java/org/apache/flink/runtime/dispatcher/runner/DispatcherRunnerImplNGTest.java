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

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link DispatcherRunnerImplNG}.
 */
public class DispatcherRunnerImplNGTest extends TestLogger {

	private TestingLeaderElectionService testingLeaderElectionService;
	private TestingFatalErrorHandler testingFatalErrorHandler;
	private TestingDispatcherLeaderProcessFactory testingDispatcherLeaderProcessFactory;

	@Before
	public void setup() {
		testingLeaderElectionService = new TestingLeaderElectionService();
		testingFatalErrorHandler = new TestingFatalErrorHandler();
		testingDispatcherLeaderProcessFactory = TestingDispatcherLeaderProcessFactory.defaultValue();
	}

	@After
	public void teardown() throws Exception {
		if (testingLeaderElectionService != null) {
			testingLeaderElectionService.stop();
			testingLeaderElectionService = null;
		}

		if (testingFatalErrorHandler != null) {
			testingFatalErrorHandler.rethrowError();
			testingFatalErrorHandler = null;
		}
	}

	@Test
	public void closeAsync_withUncompletedShutDownFuture_completesShutDownFuture() throws Exception {
		final DispatcherRunnerImplNG dispatcherRunner = createDispatcherRunner();

		final CompletableFuture<Void> terminationFuture = dispatcherRunner.closeAsync();
		terminationFuture.get();

		final CompletableFuture<ApplicationStatus> shutDownFuture = dispatcherRunner.getShutDownFuture();
		assertThat(shutDownFuture.isDone(), is(true));
		assertThat(shutDownFuture.get(), is(ApplicationStatus.UNKNOWN));
	}

	@Test
	public void getDispatcherGateway_beforeDispatcherLeaderProcessCompletes_returnsDispatcherGateway() throws Exception {
		final UUID leaderSessionId = UUID.randomUUID();
		final TestingDispatcherGateway expectedDispatcherGateway = createDispatcherGateway(leaderSessionId);
		final TestingDispatcherLeaderProcess testingDispatcherLeaderProcess = TestingDispatcherLeaderProcess.newBuilder(leaderSessionId)
			.setDispatcherGatewayFuture(CompletableFuture.completedFuture(expectedDispatcherGateway))
			.build();

		testingDispatcherLeaderProcessFactory = TestingDispatcherLeaderProcessFactory.from(testingDispatcherLeaderProcess);
		try (final DispatcherRunnerImplNG dispatcherRunner = createDispatcherRunner()) {

			final CompletableFuture<DispatcherGateway> dispatcherGatewayFuture = dispatcherRunner.getDispatcherGateway();

			assertThat(dispatcherGatewayFuture.isDone(), is(false));

			testingLeaderElectionService.isLeader(leaderSessionId);

			assertThat(dispatcherGatewayFuture.get(), is(expectedDispatcherGateway));
		}
	}

	@Test
	public void getDispatcherGateway_withChangingLeaders_returnsLeadingDispatcherGateway() throws Exception {
		final UUID firstLeaderSessionId = UUID.randomUUID();
		final UUID secondLeaderSessionId = UUID.randomUUID();
		final TestingDispatcherGateway firstDispatcherGateway = createDispatcherGateway(firstLeaderSessionId);
		final TestingDispatcherGateway secondDispatcherGateway = createDispatcherGateway(secondLeaderSessionId);

		final TestingDispatcherLeaderProcess firstDispatcherLeaderProcess = TestingDispatcherLeaderProcess.newBuilder(firstLeaderSessionId)
			.setDispatcherGatewayFuture(CompletableFuture.completedFuture(firstDispatcherGateway))
			.build();
		final TestingDispatcherLeaderProcess secondDispatcherLeaderProcess = TestingDispatcherLeaderProcess.newBuilder(secondLeaderSessionId)
			.setDispatcherGatewayFuture(CompletableFuture.completedFuture(secondDispatcherGateway))
			.build();

		testingDispatcherLeaderProcessFactory = TestingDispatcherLeaderProcessFactory.from(
			firstDispatcherLeaderProcess,
			secondDispatcherLeaderProcess);

		try (final DispatcherRunnerImplNG dispatcherRunner = createDispatcherRunner()) {
			testingLeaderElectionService.isLeader(firstLeaderSessionId);

			final CompletableFuture<DispatcherGateway> firstDispatcherGatewayFuture = dispatcherRunner.getDispatcherGateway();

			testingLeaderElectionService.notLeader();
			testingLeaderElectionService.isLeader(secondLeaderSessionId);

			final CompletableFuture<DispatcherGateway> secondDispatcherGatewayFuture = dispatcherRunner.getDispatcherGateway();

			assertThat(firstDispatcherGatewayFuture.get(), is(firstDispatcherGateway));
			assertThat(secondDispatcherGatewayFuture.get(), is(secondDispatcherGateway));
		}
	}

	@Test
	public void revokeLeadership_withExistingLeader_stopsLeaderProcess() throws Exception {
		final UUID leaderSessionId = UUID.randomUUID();

		final CompletableFuture<Void> startFuture = new CompletableFuture<>();
		final CompletableFuture<Void> stopFuture = new CompletableFuture<>();
		testingDispatcherLeaderProcessFactory = TestingDispatcherLeaderProcessFactory.from(
			TestingDispatcherLeaderProcess.newBuilder(leaderSessionId)
				.setStartConsumer(startFuture::complete)
				.setCloseAsyncSupplier(
					() -> {
						stopFuture.complete(null);
						return FutureUtils.completedVoidFuture();
					})
				.build());
		try (final DispatcherRunnerImplNG dispatcherRunner = createDispatcherRunner()) {
			testingLeaderElectionService.isLeader(leaderSessionId);

			// wait until the leader process has been started
			startFuture.get();

			testingLeaderElectionService.notLeader();

			// verify that the leader gets stopped
			stopFuture.get();
		}
	}

	@Test
	public void grantLeadership_withExistingLeader_waitsForTerminationOfFirstLeader() throws Exception {
		final UUID firstLeaderSessionId = UUID.randomUUID();
		final UUID secondLeaderSessionId = UUID.randomUUID();

		final StartStopTestingDispatcherLeaderProcess firstTestingDispatcherLeaderProcess = StartStopTestingDispatcherLeaderProcess.create(firstLeaderSessionId);
		final StartStopTestingDispatcherLeaderProcess secondTestingDispatcherLeaderProcess = StartStopTestingDispatcherLeaderProcess.create(secondLeaderSessionId);

		testingDispatcherLeaderProcessFactory = TestingDispatcherLeaderProcessFactory.from(
			firstTestingDispatcherLeaderProcess.asTestingDispatcherLeaderProcess(),
			secondTestingDispatcherLeaderProcess.asTestingDispatcherLeaderProcess());

		try (final DispatcherRunnerImplNG dispatcherRunner = createDispatcherRunner()) {
			testingLeaderElectionService.isLeader(firstLeaderSessionId);

			assertThat(firstTestingDispatcherLeaderProcess.isStarted(), is(true));

			testingLeaderElectionService.isLeader(secondLeaderSessionId);

			assertThat(secondTestingDispatcherLeaderProcess.isStarted(), is(false));
			firstTestingDispatcherLeaderProcess.terminateProcess();
			assertThat(secondTestingDispatcherLeaderProcess.isStarted(), is(true));
			secondTestingDispatcherLeaderProcess.terminateProcess(); // make the dispatcherRunner terminate
		}
	}

	@Test
	public void grantLeadership_validLeader_confirmsLeaderSession() throws Exception {
		final UUID leaderSessionId = UUID.randomUUID();

		try (final DispatcherRunnerImplNG dispatcherRunner = createDispatcherRunner()) {
			testingLeaderElectionService.isLeader(leaderSessionId);

			final CompletableFuture<LeaderConnectionInfo> confirmationFuture = testingLeaderElectionService.getConfirmationFuture();

			final LeaderConnectionInfo leaderConnectionInfo = confirmationFuture.get();
			assertThat(leaderConnectionInfo.getLeaderSessionId(), is(leaderSessionId));
		}
	}

	@Test
	public void grantLeadership_oldLeader_doesNotConfirmLeaderSession() throws Exception {
		final UUID leaderSessionId = UUID.randomUUID();
		final CompletableFuture<String> contenderConfirmationFuture = new CompletableFuture<>();
		final TestingDispatcherLeaderProcess testingDispatcherLeaderProcess = TestingDispatcherLeaderProcess.newBuilder(leaderSessionId)
			.setConfirmLeaderSessionFuture(contenderConfirmationFuture)
			.build();

		testingDispatcherLeaderProcessFactory = TestingDispatcherLeaderProcessFactory.from(testingDispatcherLeaderProcess);

		try (final DispatcherRunnerImplNG dispatcherRunner = createDispatcherRunner()) {
			testingLeaderElectionService.isLeader(leaderSessionId);

			testingLeaderElectionService.notLeader();

			// complete the confirmation future after losing the leadership
			contenderConfirmationFuture.complete("leader address");

			final CompletableFuture<LeaderConnectionInfo> leaderElectionConfirmationFuture = testingLeaderElectionService.getConfirmationFuture();

			try {
				leaderElectionConfirmationFuture.get(5L, TimeUnit.MILLISECONDS);
				fail("No valid leader should exist.");
			} catch (TimeoutException expected) {}
		}
	}

	@Test
	public void grantLeadership_multipleLeaderChanges_lastDispatcherLeaderProcessWaitsForOthersToTerminateBeforeItStarts() throws Exception {
		final UUID firstLeaderSession = UUID.randomUUID();
		final UUID secondLeaderSession = UUID.randomUUID();
		final UUID thirdLeaderSession = UUID.randomUUID();

		final CompletableFuture<Void> firstDispatcherLeaderProcessTerminationFuture = new CompletableFuture<>();
		final TestingDispatcherLeaderProcess firstDispatcherLeaderProcess = TestingDispatcherLeaderProcess.newBuilder(firstLeaderSession)
			.setCloseAsyncSupplier(() -> firstDispatcherLeaderProcessTerminationFuture)
			.build();
		final CompletableFuture<Void> secondDispatcherLeaderProcessTerminationFuture = new CompletableFuture<>();
		final TestingDispatcherLeaderProcess secondDispatcherLeaderProcess = TestingDispatcherLeaderProcess.newBuilder(secondLeaderSession)
			.setCloseAsyncSupplier(() -> secondDispatcherLeaderProcessTerminationFuture)
			.build();
		final CompletableFuture<Void> thirdDispatcherLeaderProcessHasBeenStartedFuture = new CompletableFuture<>();
		final TestingDispatcherLeaderProcess thirdDispatcherLeaderProcess = TestingDispatcherLeaderProcess.newBuilder(thirdLeaderSession)
			.setStartConsumer(thirdDispatcherLeaderProcessHasBeenStartedFuture::complete)
			.build();

		testingDispatcherLeaderProcessFactory = TestingDispatcherLeaderProcessFactory.from(
			firstDispatcherLeaderProcess,
			secondDispatcherLeaderProcess,
			thirdDispatcherLeaderProcess);

		final DispatcherRunnerImplNG dispatcherRunner = createDispatcherRunner();

		try {
			testingLeaderElectionService.isLeader(firstLeaderSession);
			testingLeaderElectionService.isLeader(secondLeaderSession);
			testingLeaderElectionService.isLeader(thirdLeaderSession);

			firstDispatcherLeaderProcessTerminationFuture.complete(null);

			assertThat(thirdDispatcherLeaderProcessHasBeenStartedFuture.isDone(), is(false));

			secondDispatcherLeaderProcessTerminationFuture.complete(null);

			assertThat(thirdDispatcherLeaderProcessHasBeenStartedFuture.isDone(), is(true));
		} finally {
			firstDispatcherLeaderProcessTerminationFuture.complete(null);
			secondDispatcherLeaderProcessTerminationFuture.complete(null);

			dispatcherRunner.close();
		}
	}

	private static class StartStopTestingDispatcherLeaderProcess {

		private final TestingDispatcherLeaderProcess testingDispatcherLeaderProcess;
		private final CompletableFuture<Void> startFuture;
		private final CompletableFuture<Void> terminationFuture;

		private StartStopTestingDispatcherLeaderProcess(
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

		private static StartStopTestingDispatcherLeaderProcess create(UUID leaderSessionId) {
			final CompletableFuture<Void> processStartFuture = new CompletableFuture<>();
			final CompletableFuture<Void> processTerminationFuture = new CompletableFuture<>();
			final TestingDispatcherLeaderProcess dispatcherLeaderProcess = TestingDispatcherLeaderProcess.newBuilder(leaderSessionId)
				.setStartConsumer(processStartFuture::complete)
				.setCloseAsyncSupplier(() -> processTerminationFuture)
				.build();

			return new StartStopTestingDispatcherLeaderProcess(dispatcherLeaderProcess, processStartFuture, processTerminationFuture);
		}
	}

	private TestingDispatcherGateway createDispatcherGateway(UUID leaderSessionId) {
		return new TestingDispatcherGateway.Builder()
			.setFencingToken(DispatcherId.fromUuid(leaderSessionId))
			.build();
	}

	private DispatcherRunnerImplNG createDispatcherRunner() throws Exception {
		return new DispatcherRunnerImplNG(
			testingLeaderElectionService,
			testingFatalErrorHandler,
			testingDispatcherLeaderProcessFactory);
	}
}
