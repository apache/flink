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

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.leaderelection.TestingListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.rest.handler.util.MimeTypes;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.AkkaJobManagerRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.AkkaQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.testutils.HttpTestClient;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.apache.curator.test.TestingServer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.configuration.ConfigConstants.LOCAL_START_WEBSERVER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for the WebRuntimeMonitor.
 */
public class WebRuntimeMonitorITCase extends TestLogger {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final FiniteDuration TEST_TIMEOUT = new FiniteDuration(2L, TimeUnit.MINUTES);

	private static final Time TIMEOUT = Time.milliseconds(TEST_TIMEOUT.toMillis());

	private final String mainResourcesPath = getClass().getResource("/web").getPath();

	/**
	 * Tests operation of the monitor in standalone operation.
	 */
	@Test
	public void testStandaloneWebRuntimeMonitor() throws Exception {
		final Deadline deadline = TEST_TIMEOUT.fromNow();

		TestingCluster flink = null;

		final Configuration configuration = new Configuration();

		// activate the web monitor
		configuration.setBoolean(LOCAL_START_WEBSERVER, true);
		configuration.setInteger(WebOptions.PORT, 0);

		try {
			// Flink with a web monitor
			flink = new TestingCluster(configuration);
			flink.start(true);

			WebMonitor webMonitor = flink.webMonitor().get();

			try (HttpTestClient client = new HttpTestClient("localhost", webMonitor.getServerPort())) {
				String expected = new Scanner(new File(mainResourcesPath + "/index.html"))
						.useDelimiter("\\A").next();

				// Request the file from the web server
				client.sendGetRequest("index.html", deadline.timeLeft());

				HttpTestClient.SimpleHttpResponse response = client.getNextResponse(deadline.timeLeft());

				assertEquals(HttpResponseStatus.OK, response.getStatus());
				assertEquals(response.getType(), MimeTypes.getMimeTypeForExtension("html"));
				assertEquals(expected, response.getContent());

				// Simple overview request
				client.sendGetRequest("/overview", deadline.timeLeft());

				response = client.getNextResponse(deadline.timeLeft());
				assertEquals(HttpResponseStatus.OK, response.getStatus());
				assertEquals("application/json; charset=UTF-8", response.getType());
				assertTrue(response.getContent().contains("\"taskmanagers\":1"));
			}
		}
		finally {
			if (flink != null) {
				flink.stop();
			}
		}
	}

	/**
	 * Tests that the monitor associated with the following job manager redirects to the leader.
	 */
	@Test
	public void testRedirectToLeader() throws Exception {
		final Deadline deadline = TEST_TIMEOUT.fromNow();

		ActorSystem[] jobManagerSystem = new ActorSystem[2];
		WebRuntimeMonitor[] webMonitor = new WebRuntimeMonitor[2];
		AkkaJobManagerRetriever[] jobManagerRetrievers = new AkkaJobManagerRetriever[2];
		HighAvailabilityServices highAvailabilityServices = null;

		try (TestingServer zooKeeper = new TestingServer()) {
			final Configuration config = ZooKeeperTestUtils.createZooKeeperHAConfig(
				zooKeeper.getConnectString(),
				temporaryFolder.getRoot().getPath());

			File logDir = temporaryFolder.newFolder();
			Path logFile = Files.createFile(new File(logDir, "jobmanager.log").toPath());
			Files.createFile(new File(logDir, "jobmanager.out").toPath());

			config.setInteger(WebOptions.PORT, 0);
			config.setString(WebOptions.LOG_PATH, logFile.toString());

			highAvailabilityServices = HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(
				config,
				TestingUtils.defaultExecutor());

			for (int i = 0; i < jobManagerSystem.length; i++) {
				jobManagerSystem[i] = AkkaUtils.createActorSystem(new Configuration(),
						new Some<>(new Tuple2<String, Object>("localhost", 0)));
			}

			for (int i = 0; i < webMonitor.length; i++) {
				jobManagerRetrievers[i] = new AkkaJobManagerRetriever(jobManagerSystem[i], TIMEOUT, 0, Time.milliseconds(50L));

				webMonitor[i] = new WebRuntimeMonitor(
					config,
					highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
					jobManagerRetrievers[i],
					new AkkaQueryServiceRetriever(jobManagerSystem[i], TIMEOUT),
					TIMEOUT,
					TestingUtils.defaultScheduledExecutor());
			}

			ActorRef[] jobManager = new ActorRef[2];
			String[] jobManagerAddress = new String[2];
			for (int i = 0; i < jobManager.length; i++) {
				Configuration jmConfig = config.clone();
				jmConfig.setInteger(WebOptions.PORT,
						webMonitor[i].getServerPort());

				jobManager[i] = JobManager.startJobManagerActors(
					jmConfig,
					jobManagerSystem[i],
					TestingUtils.defaultExecutor(),
					TestingUtils.defaultExecutor(),
					highAvailabilityServices,
					NoOpMetricRegistry.INSTANCE,
					Option.apply(webMonitor[i].getRestAddress()),
					JobManager.class,
					MemoryArchivist.class)._1();

				jobManagerAddress[i] = AkkaUtils.getAkkaURL(jobManagerSystem[i], jobManager[i]);
				webMonitor[i].start();
			}

			LeaderRetrievalService lrs = highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID);
			TestingListener leaderListener = new TestingListener();
			lrs.start(leaderListener);

			leaderListener.waitForNewLeader(deadline.timeLeft().toMillis());

			String leaderAddress = leaderListener.getAddress();

			int leaderIndex = leaderAddress.equals(jobManagerAddress[0]) ? 0 : 1;
			int followerIndex = (leaderIndex + 1) % 2;

			ActorSystem leadingSystem = jobManagerSystem[leaderIndex];

			WebMonitor leadingWebMonitor = webMonitor[leaderIndex];
			WebMonitor followerWebMonitor = webMonitor[followerIndex];

			// For test stability reason we have to wait until we are sure that both leader
			// listeners have been notified.
			AkkaJobManagerRetriever leadingRetriever = jobManagerRetrievers[leaderIndex];
			AkkaJobManagerRetriever followerRetriever = jobManagerRetrievers[followerIndex];

			// Wait for the initial notifications
			waitForLeaderNotification(jobManager[leaderIndex].path().toString(), leadingRetriever, deadline);
			waitForLeaderNotification(AkkaUtils.getAkkaURL(leadingSystem, jobManager[leaderIndex]), followerRetriever, deadline);

			try (
					HttpTestClient leaderClient = new HttpTestClient(
							"localhost", leadingWebMonitor.getServerPort());

					HttpTestClient followingClient = new HttpTestClient(
							"localhost", followerWebMonitor.getServerPort())) {

				String expected = new Scanner(new File(mainResourcesPath + "/index.html"))
						.useDelimiter("\\A").next();

				// Request the file from the leading web server
				leaderClient.sendGetRequest("index.html", deadline.timeLeft());

				HttpTestClient.SimpleHttpResponse response = leaderClient.getNextResponse(deadline.timeLeft());
				assertEquals(HttpResponseStatus.OK, response.getStatus());
				assertEquals(response.getType(), MimeTypes.getMimeTypeForExtension("html"));
				assertEquals(expected, response.getContent());

				// Request the file from the following web server
				followingClient.sendGetRequest("index.html", deadline.timeLeft());
				response = followingClient.getNextResponse(deadline.timeLeft());
				assertEquals(HttpResponseStatus.TEMPORARY_REDIRECT, response.getStatus());
				assertTrue(response.getLocation().contains(String.valueOf(leadingWebMonitor.getServerPort())));

				// Kill the leader
				leadingSystem.shutdown();

				// Wait for the notification of the follower
				waitForLeaderNotification(jobManager[followerIndex].path().toString(), followerRetriever, deadline);

				// Same request to the new leader
				followingClient.sendGetRequest("index.html", deadline.timeLeft());

				response = followingClient.getNextResponse(deadline.timeLeft());
				assertEquals(HttpResponseStatus.OK, response.getStatus());
				assertEquals(response.getType(), MimeTypes.getMimeTypeForExtension("html"));
				assertEquals(expected, response.getContent());

				// Simple overview request
				followingClient.sendGetRequest("/overview", deadline.timeLeft());

				response = followingClient.getNextResponse(deadline.timeLeft());
				assertEquals(HttpResponseStatus.OK, response.getStatus());
				assertEquals("application/json; charset=UTF-8", response.getType());
				assertTrue(response.getContent().contains("\"taskmanagers\":1") ||
						response.getContent().contains("\"taskmanagers\":0"));
			} finally {
				lrs.stop();
			}
		}
		finally {
			for (ActorSystem system : jobManagerSystem) {
				if (system != null) {
					system.shutdown();
				}
			}

			for (WebMonitor monitor : webMonitor) {
				monitor.stop();
			}

			if (highAvailabilityServices != null) {
				highAvailabilityServices.closeAndCleanupAllData();
			}
		}
	}

	@Test
	public void testLeaderNotAvailable() throws Exception {
		final Deadline deadline = TEST_TIMEOUT.fromNow();

		ActorSystem actorSystem = null;
		WebRuntimeMonitor webRuntimeMonitor = null;

		try (TestingServer zooKeeper = new TestingServer()) {

			File logDir = temporaryFolder.newFolder();
			Path logFile = Files.createFile(new File(logDir, "jobmanager.log").toPath());
			Files.createFile(new File(logDir, "jobmanager.out").toPath());

			final Configuration config = new Configuration();
			config.setInteger(WebOptions.PORT, 0);
			config.setString(WebOptions.LOG_PATH, logFile.toString());
			config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
			config.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeper.getConnectString());

			actorSystem = AkkaUtils.createDefaultActorSystem();

			webRuntimeMonitor = new WebRuntimeMonitor(
				config,
				mock(LeaderRetrievalService.class),
				new AkkaJobManagerRetriever(actorSystem, TIMEOUT, 0, Time.milliseconds(50L)),
				new AkkaQueryServiceRetriever(actorSystem, TIMEOUT),
				TIMEOUT,
				TestingUtils.defaultScheduledExecutor());

			webRuntimeMonitor.start();

			try (HttpTestClient client = new HttpTestClient(
					"localhost", webRuntimeMonitor.getServerPort())) {

				client.sendGetRequest("index.html", deadline.timeLeft());

				HttpTestClient.SimpleHttpResponse response = client.getNextResponse();

				assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE, response.getStatus());
				assertEquals("application/json; charset=UTF-8", response.getType());
				assertTrue(response.getContent().contains("refresh"));
			}
		}
		finally {
			if (actorSystem != null) {
				actorSystem.shutdown();
			}

			if (webRuntimeMonitor != null) {
				webRuntimeMonitor.stop();
			}
		}
	}

	// ------------------------------------------------------------------------
	// Tests that access outside of the web root is not allowed
	// ------------------------------------------------------------------------

	/**
	 * Files are copied from the flink-dist jar to a temporary directory and
	 * then served from there. Only allow to access files in this temporary
	 * directory.
	 */
	@Test
	public void testNoEscape() throws Exception {
		final Deadline deadline = TEST_TIMEOUT.fromNow();

		TestingCluster flink = null;

		final Configuration configuration = new Configuration();

		// activate the web monitor
		configuration.setBoolean(LOCAL_START_WEBSERVER, true);
		configuration.setInteger(WebOptions.PORT, 0);

		try {
			flink = new TestingCluster(configuration);
			flink.start(true);

			WebRuntimeMonitor webMonitor = (WebRuntimeMonitor) flink.webMonitor().get();

			try (HttpTestClient client = new HttpTestClient("localhost", webMonitor.getServerPort())) {
				String expectedIndex = new Scanner(new File(mainResourcesPath + "/index.html"))
						.useDelimiter("\\A").next();

				// 1) Request index.html from web server
				client.sendGetRequest("index.html", deadline.timeLeft());

				HttpTestClient.SimpleHttpResponse response = client.getNextResponse(deadline.timeLeft());
				assertEquals(HttpResponseStatus.OK, response.getStatus());
				assertEquals(response.getType(), MimeTypes.getMimeTypeForExtension("html"));
				assertEquals(expectedIndex, response.getContent());

				// 2) Request file outside of web root
				// Create a test file in the web base dir (parent of web root)
				File illegalFile = new File(webMonitor.getBaseDir(new Configuration()), "test-file-" + UUID.randomUUID());
				illegalFile.deleteOnExit();

				assertTrue("Failed to create test file", illegalFile.createNewFile());

				// Request the created file from the web server
				client.sendGetRequest("../" + illegalFile.getName(), deadline.timeLeft());
				response = client.getNextResponse(deadline.timeLeft());
				assertEquals(
						"Unexpected status code " + response.getStatus() + " for file outside of web root.",
						HttpResponseStatus.NOT_FOUND,
						response.getStatus());

				// 3) Request non-existing file
				client.sendGetRequest("not-existing-resource", deadline.timeLeft());
				response = client.getNextResponse(deadline.timeLeft());
				assertEquals(
						"Unexpected status code " + response.getStatus() + " for file outside of web root.",
						HttpResponseStatus.NOT_FOUND,
						response.getStatus());
			}
		} finally {
			if (flink != null) {
				flink.stop();
			}
		}
	}

	/**
	 * Files are copied from the flink-dist jar to a temporary directory and
	 * then served from there. Only allow to copy files from <code>flink-dist.jar:/web</code>
	 */
	@Test
	public void testNoCopyFromJar() throws Exception {
		final Deadline deadline = TEST_TIMEOUT.fromNow();

		TestingCluster flink = null;

		final Configuration configuration = new Configuration();

		// activate the web monitor
		configuration.setBoolean(LOCAL_START_WEBSERVER, true);
		configuration.setInteger(WebOptions.PORT, 0);

		try {
			flink = new TestingCluster(configuration);
			flink.start(true);
			WebRuntimeMonitor webMonitor = ((WebRuntimeMonitor) flink.webMonitor().get());

			try (HttpTestClient client = new HttpTestClient("localhost", webMonitor.getServerPort())) {
				String expectedIndex = new Scanner(new File(mainResourcesPath + "/index.html"))
					.useDelimiter("\\A").next();

				// 1) Request index.html from web server
				client.sendGetRequest("index.html", deadline.timeLeft());

				HttpTestClient.SimpleHttpResponse response = client.getNextResponse(deadline.timeLeft());
				assertEquals(HttpResponseStatus.OK, response.getStatus());
				assertEquals(response.getType(), MimeTypes.getMimeTypeForExtension("html"));
				assertEquals(expectedIndex, response.getContent());

				// 2) Request file from class loader
				client.sendGetRequest("../log4j-test.properties", deadline.timeLeft());

				response = client.getNextResponse(deadline.timeLeft());
				assertEquals(
					"Returned status code " + response.getStatus() + " for file outside of web root.",
					HttpResponseStatus.NOT_FOUND,
					response.getStatus());

				assertFalse("Did not respond with the file, but still copied it from the JAR.",
					new File(webMonitor.getBaseDir(new Configuration()), "log4j-test.properties").exists());

				// 3) Request non-existing file
				client.sendGetRequest("not-existing-resource", deadline.timeLeft());
				response = client.getNextResponse(deadline.timeLeft());
				assertEquals(
					"Unexpected status code " + response.getStatus() + " for file outside of web root.",
					HttpResponseStatus.NOT_FOUND,
					response.getStatus());
			}
		} finally {
			if (flink != null) {
				flink.stop();
			}
		}
	}

	// ------------------------------------------------------------------------

	private void waitForLeaderNotification(
			String expectedJobManagerURL,
			GatewayRetriever<JobManagerGateway> retriever,
			Deadline deadline) throws Exception {

		while (deadline.hasTimeLeft()) {
			Optional<JobManagerGateway> optJobManagerGateway = retriever.getNow();

			if (optJobManagerGateway.isPresent() && Objects.equals(expectedJobManagerURL, optJobManagerGateway.get().getAddress())) {
				return;
			}
			else {
				Thread.sleep(100);
			}
		}
	}
}
