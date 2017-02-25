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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.curator.test.TestingServer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.leaderelection.TestingListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.webmonitor.files.MimeTypes;
import org.apache.flink.runtime.webmonitor.testutils.HttpTestClient;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.powermock.reflect.Whitebox;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class WebRuntimeMonitorITCase extends TestLogger {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private final static FiniteDuration TestTimeout = new FiniteDuration(2, TimeUnit.MINUTES);

	private final String MAIN_RESOURCES_PATH = getClass().getResource("/web").getPath();

	/**
	 * Tests operation of the monitor in standalone operation.
	 */
	@Test
	public void testStandaloneWebRuntimeMonitor() throws Exception {
		final Deadline deadline = TestTimeout.fromNow();

		TestingCluster flink = null;
		WebRuntimeMonitor webMonitor = null;

		try {
			// Flink w/o a web monitor
			flink = new TestingCluster(new Configuration());
			flink.start(true);
			webMonitor = startWebRuntimeMonitor(flink);

			try (HttpTestClient client = new HttpTestClient("localhost", webMonitor.getServerPort())) {
				String expected = new Scanner(new File(MAIN_RESOURCES_PATH + "/index.html"))
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
				assertEquals(response.getType(), MimeTypes.getMimeTypeForExtension("json"));
				assertTrue(response.getContent().contains("\"taskmanagers\":1"));
			}
		}
		finally {
			if (flink != null) {
				flink.shutdown();
			}

			if (webMonitor != null) {
				webMonitor.stop();
			}
		}
	}

	/**
	 * Tests that the monitor associated with the following job manager redirects to the leader.
	 */
	@Test
	public void testRedirectToLeader() throws Exception {
		final Deadline deadline = TestTimeout.fromNow();

		ActorSystem[] jobManagerSystem = new ActorSystem[2];
		WebRuntimeMonitor[] webMonitor = new WebRuntimeMonitor[2];
		List<LeaderRetrievalService> leaderRetrievalServices = new ArrayList<>();

		try (TestingServer zooKeeper = new TestingServer()) {
			final Configuration config = ZooKeeperTestUtils.createZooKeeperHAConfig(
				zooKeeper.getConnectString(),
				temporaryFolder.getRoot().getPath());

			File logDir = temporaryFolder.newFolder();
			Path logFile = Files.createFile(new File(logDir, "jobmanager.log").toPath());
			Files.createFile(new File(logDir, "jobmanager.out").toPath());

			config.setString(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, "0");
			config.setString(ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY, logFile.toString());

			for (int i = 0; i < jobManagerSystem.length; i++) {
				jobManagerSystem[i] = AkkaUtils.createActorSystem(new Configuration(),
						new Some<>(new Tuple2<String, Object>("localhost", 0)));
			}

			for (int i = 0; i < webMonitor.length; i++) {
				LeaderRetrievalService lrs = ZooKeeperUtils.createLeaderRetrievalService(config);
				leaderRetrievalServices.add(lrs);
				webMonitor[i] = new WebRuntimeMonitor(config, lrs, jobManagerSystem[i]);
			}

			ActorRef[] jobManager = new ActorRef[2];
			String[] jobManagerAddress = new String[2];
			for (int i = 0; i < jobManager.length; i++) {
				Configuration jmConfig = config.clone();
				jmConfig.setString(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY,
						String.valueOf(webMonitor[i].getServerPort()));

				jobManager[i] = JobManager.startJobManagerActors(
					jmConfig,
					jobManagerSystem[i],
					TestingUtils.defaultExecutor(),
					TestingUtils.defaultExecutor(),
					JobManager.class,
					MemoryArchivist.class)._1();

				jobManagerAddress[i] = AkkaUtils.getAkkaURL(jobManagerSystem[i], jobManager[i]);
				webMonitor[i].start(jobManagerAddress[i]);
			}

			LeaderRetrievalService lrs = ZooKeeperUtils.createLeaderRetrievalService(config);
			leaderRetrievalServices.add(lrs);
			TestingListener leaderListener = new TestingListener();
			lrs.start(leaderListener);

			leaderListener.waitForNewLeader(deadline.timeLeft().toMillis());

			String leaderAddress = leaderListener.getAddress();

			int leaderIndex = leaderAddress.equals(jobManagerAddress[0]) ? 0 : 1;
			int followerIndex = (leaderIndex + 1) % 2;

			ActorSystem leadingSystem = jobManagerSystem[leaderIndex];
			ActorSystem followerSystem = jobManagerSystem[followerIndex];

			WebMonitor leadingWebMonitor = webMonitor[leaderIndex];
			WebMonitor followerWebMonitor = webMonitor[followerIndex];

			// For test stability reason we have to wait until we are sure that both leader
			// listeners have been notified.
			JobManagerRetriever leadingRetriever = Whitebox
					.getInternalState(leadingWebMonitor, "retriever");

			JobManagerRetriever followerRetriever = Whitebox
					.getInternalState(followerWebMonitor, "retriever");

			// Wait for the initial notifications
			waitForLeaderNotification(leadingSystem, jobManager[leaderIndex], leadingRetriever, deadline);
			waitForLeaderNotification(leadingSystem, jobManager[leaderIndex], followerRetriever, deadline);

			try (
					HttpTestClient leaderClient = new HttpTestClient(
							"localhost", leadingWebMonitor.getServerPort());

					HttpTestClient followingClient = new HttpTestClient(
							"localhost", followerWebMonitor.getServerPort())) {

				String expected = new Scanner(new File(MAIN_RESOURCES_PATH + "/index.html"))
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
				waitForLeaderNotification(followerSystem, jobManager[followerIndex], followerRetriever, deadline);

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
				assertEquals(response.getType(), MimeTypes.getMimeTypeForExtension("json"));
				assertTrue(response.getContent().contains("\"taskmanagers\":1") ||
						response.getContent().contains("\"taskmanagers\":0"));
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

			for (LeaderRetrievalService lrs : leaderRetrievalServices) {
				lrs.stop();
			}
		}
	}

	@Test
	public void testLeaderNotAvailable() throws Exception {
		final Deadline deadline = TestTimeout.fromNow();

		ActorSystem actorSystem = null;
		WebRuntimeMonitor webRuntimeMonitor = null;

		try (TestingServer zooKeeper = new TestingServer()) {

			File logDir = temporaryFolder.newFolder();
			Path logFile = Files.createFile(new File(logDir, "jobmanager.log").toPath());
			Files.createFile(new File(logDir, "jobmanager.out").toPath());

			final Configuration config = new Configuration();
			config.setString(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, "0");
			config.setString(ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY, logFile.toString());
			config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
			config.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeper.getConnectString());

			actorSystem = AkkaUtils.createDefaultActorSystem();

			LeaderRetrievalService leaderRetrievalService = mock(LeaderRetrievalService.class);
			webRuntimeMonitor = new WebRuntimeMonitor(
					config, leaderRetrievalService, actorSystem);

			webRuntimeMonitor.start("akka://schmakka");

			try (HttpTestClient client = new HttpTestClient(
					"localhost", webRuntimeMonitor.getServerPort())) {

				client.sendGetRequest("index.html", deadline.timeLeft());

				HttpTestClient.SimpleHttpResponse response = client.getNextResponse();

				assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE, response.getStatus());
				assertEquals(MimeTypes.getMimeTypeForExtension("txt"), response.getType());
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
		final Deadline deadline = TestTimeout.fromNow();

		TestingCluster flink = null;
		WebRuntimeMonitor webMonitor = null;

		try {
			flink = new TestingCluster(new Configuration());
			flink.start(true);
			webMonitor = startWebRuntimeMonitor(flink);

			try (HttpTestClient client = new HttpTestClient("localhost", webMonitor.getServerPort())) {
				String expectedIndex = new Scanner(new File(MAIN_RESOURCES_PATH + "/index.html"))
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
				flink.shutdown();
			}

			if (webMonitor != null) {
				webMonitor.stop();
			}
		}
	}

	/**
	 * Files are copied from the flink-dist jar to a temporary directory and
	 * then served from there. Only allow to copy files from <code>flink-dist.jar:/web</code>
	 */
	@Test
	public void testNoCopyFromJar() throws Exception {
		final Deadline deadline = TestTimeout.fromNow();

		TestingCluster flink = null;
		WebRuntimeMonitor webMonitor = null;

		try {
			flink = new TestingCluster(new Configuration());
			flink.start(true);
			webMonitor = startWebRuntimeMonitor(flink);

			try (HttpTestClient client = new HttpTestClient("localhost", webMonitor.getServerPort())) {
				String expectedIndex = new Scanner(new File(MAIN_RESOURCES_PATH + "/index.html"))
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
				flink.shutdown();
			}

			if (webMonitor != null) {
				webMonitor.stop();
			}
		}
	}

	private WebRuntimeMonitor startWebRuntimeMonitor(
		TestingCluster flink) throws Exception {

		ActorSystem jmActorSystem = flink.jobManagerActorSystems().get().head();
		ActorRef jmActor = flink.jobManagerActors().get().head();

		// Needs to match the leader address from the leader retrieval service
		String jobManagerAddress = AkkaUtils.getAkkaURL(jmActorSystem, jmActor);

		File logDir = temporaryFolder.newFolder("log");
		Path logFile = Files.createFile(new File(logDir, "jobmanager.log").toPath());
		Files.createFile(new File(logDir, "jobmanager.out").toPath());

		// Web frontend on random port
		Configuration config = new Configuration();
		config.setString(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, "0");
		config.setString(ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY, logFile.toString());

		WebRuntimeMonitor webMonitor = new WebRuntimeMonitor(
			config,
			flink.createLeaderRetrievalService(),
			jmActorSystem);

		webMonitor.start(jobManagerAddress);
		flink.waitForActorsToBeAlive();
		return webMonitor;
	}

	// ------------------------------------------------------------------------

	private void waitForLeaderNotification(
			ActorSystem system,
			ActorRef expectedLeader,
			JobManagerRetriever retriever,
			Deadline deadline) throws Exception {

		String expectedJobManagerUrl = AkkaUtils.getAkkaURL(system, expectedLeader);

		while (deadline.hasTimeLeft()) {
			ActorRef leaderRef = retriever.awaitJobManagerGatewayAndWebPort()._1().actor();

			if (AkkaUtils.getAkkaURL(system, leaderRef).equals(expectedJobManagerUrl)) {
				return;
			}
			else {
				Thread.sleep(100);
			}
		}
	}
}
