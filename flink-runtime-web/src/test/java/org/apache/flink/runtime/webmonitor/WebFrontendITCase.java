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

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.StoppableTask;
import org.apache.flink.runtime.webmonitor.testutils.HttpTestClient;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the WebFrontend.
 */
public class WebFrontendITCase extends TestLogger {

	private static final int NUM_TASK_MANAGERS = 2;
	private static final int NUM_SLOTS = 4;

	private static final Configuration CLUSTER_CONFIGURATION = getClusterConfiguration();

	@ClassRule
	public static final MiniClusterResource CLUSTER = new MiniClusterResource(
		new MiniClusterResourceConfiguration.Builder()
			.setConfiguration(CLUSTER_CONFIGURATION)
			.setNumberTaskManagers(NUM_TASK_MANAGERS)
			.setNumberSlotsPerTaskManager(NUM_SLOTS)
			.build());

	private static Configuration getClusterConfiguration() {
		Configuration config = new Configuration();
		try {
			File logDir = File.createTempFile("TestBaseUtils-logdir", null);
			assertTrue("Unable to delete temp file", logDir.delete());
			assertTrue("Unable to create temp directory", logDir.mkdir());
			File logFile = new File(logDir, "jobmanager.log");
			File outFile = new File(logDir, "jobmanager.out");

			Files.createFile(logFile.toPath());
			Files.createFile(outFile.toPath());

			config.setString(WebOptions.LOG_PATH, logFile.getAbsolutePath());
			config.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, logFile.getAbsolutePath());
		} catch (Exception e) {
			throw new AssertionError("Could not setup test.", e);
		}
		config.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "12m");
		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

		return config;
	}

	@After
	public void tearDown() {
		BlockingInvokable.reset();
	}

	@Test
	public void getFrontPage() {
		try {
			String fromHTTP = TestBaseUtils.getFromHTTP("http://localhost:" + CLUSTER.getWebUIPort() + "/index.html");
			String text = "Apache Flink Dashboard";
			assertTrue("Startpage should contain " + text, fromHTTP.contains(text));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testResponseHeaders() throws Exception {
		// check headers for successful json response
		URL taskManagersUrl = new URL("http://localhost:" + CLUSTER.getWebUIPort() + "/taskmanagers");
		HttpURLConnection taskManagerConnection = (HttpURLConnection) taskManagersUrl.openConnection();
		taskManagerConnection.setConnectTimeout(100000);
		taskManagerConnection.connect();
		if (taskManagerConnection.getResponseCode() >= 400) {
			// error!
			InputStream is = taskManagerConnection.getErrorStream();
			String errorMessage = IOUtils.toString(is, ConfigConstants.DEFAULT_CHARSET);
			throw new RuntimeException(errorMessage);
		}

		// we don't set the content-encoding header
		Assert.assertNull(taskManagerConnection.getContentEncoding());
		Assert.assertEquals("application/json; charset=UTF-8", taskManagerConnection.getContentType());

		// check headers in case of an error
		URL notFoundJobUrl = new URL("http://localhost:" + CLUSTER.getWebUIPort() + "/jobs/dontexist");
		HttpURLConnection notFoundJobConnection = (HttpURLConnection) notFoundJobUrl.openConnection();
		notFoundJobConnection.setConnectTimeout(100000);
		notFoundJobConnection.connect();
		if (notFoundJobConnection.getResponseCode() >= 400) {
			// we don't set the content-encoding header
			Assert.assertNull(notFoundJobConnection.getContentEncoding());
			if (CLUSTER.getCodebaseType() == TestBaseUtils.CodebaseType.NEW) {
				Assert.assertEquals("application/json; charset=UTF-8", notFoundJobConnection.getContentType());
			} else {
				Assert.assertEquals("text/plain; charset=UTF-8", notFoundJobConnection.getContentType());
			}
		} else {
			throw new RuntimeException("Request for non-existing job did not return an error.");
		}
	}

	@Test
	public void getNumberOfTaskManagers() {
		try {
			String json = TestBaseUtils.getFromHTTP("http://localhost:" + CLUSTER.getWebUIPort() + "/taskmanagers/");

			ObjectMapper mapper = new ObjectMapper();
			JsonNode response = mapper.readTree(json);
			ArrayNode taskManagers = (ArrayNode) response.get("taskmanagers");

			assertNotNull(taskManagers);
			assertEquals(NUM_TASK_MANAGERS, taskManagers.size());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void getTaskmanagers() throws Exception {
		String json = TestBaseUtils.getFromHTTP("http://localhost:" + CLUSTER.getWebUIPort() + "/taskmanagers/");

		ObjectMapper mapper = new ObjectMapper();
		JsonNode parsed = mapper.readTree(json);
		ArrayNode taskManagers = (ArrayNode) parsed.get("taskmanagers");

		assertNotNull(taskManagers);
		assertEquals(NUM_TASK_MANAGERS, taskManagers.size());

		JsonNode taskManager = taskManagers.get(0);
		assertNotNull(taskManager);
		assertEquals(NUM_SLOTS, taskManager.get("slotsNumber").asInt());
		assertTrue(taskManager.get("freeSlots").asInt() <= NUM_SLOTS);
	}

	@Test
	public void getLogAndStdoutFiles() throws Exception {
		WebMonitorUtils.LogFileLocation logFiles = WebMonitorUtils.LogFileLocation.find(CLUSTER_CONFIGURATION);

		FileUtils.writeStringToFile(logFiles.logFile, "job manager log");
		String logs = TestBaseUtils.getFromHTTP("http://localhost:" + CLUSTER.getWebUIPort() + "/jobmanager/log");
		assertTrue(logs.contains("job manager log"));

		FileUtils.writeStringToFile(logFiles.stdOutFile, "job manager out");
		logs = TestBaseUtils.getFromHTTP("http://localhost:" + CLUSTER.getWebUIPort() + "/jobmanager/stdout");
		assertTrue(logs.contains("job manager out"));
	}

	@Test
	public void getTaskManagerLogAndStdoutFiles() {
		try {
			String json = TestBaseUtils.getFromHTTP("http://localhost:" + CLUSTER.getWebUIPort() + "/taskmanagers/");

			ObjectMapper mapper = new ObjectMapper();
			JsonNode parsed = mapper.readTree(json);
			ArrayNode taskManagers = (ArrayNode) parsed.get("taskmanagers");
			JsonNode taskManager = taskManagers.get(0);
			String id = taskManager.get("id").asText();

			WebMonitorUtils.LogFileLocation logFiles = WebMonitorUtils.LogFileLocation.find(CLUSTER_CONFIGURATION);

			//we check for job manager log files, since no separate taskmanager logs exist
			FileUtils.writeStringToFile(logFiles.logFile, "job manager log");
			String logs = TestBaseUtils.getFromHTTP("http://localhost:" + CLUSTER.getWebUIPort() + "/taskmanagers/" + id + "/log");
			assertTrue(logs.contains("job manager log"));

			FileUtils.writeStringToFile(logFiles.stdOutFile, "job manager out");
			logs = TestBaseUtils.getFromHTTP("http://localhost:" + CLUSTER.getWebUIPort() + "/taskmanagers/" + id + "/stdout");
			assertTrue(logs.contains("job manager out"));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void getConfiguration() {
		try {
			String config = TestBaseUtils.getFromHTTP("http://localhost:" + CLUSTER.getWebUIPort() + "/jobmanager/config");

			Map<String, String> conf = WebMonitorUtils.fromKeyValueJsonArray(config);
			assertEquals(
				CLUSTER_CONFIGURATION.getString(ConfigConstants.LOCAL_START_WEBSERVER, null),
				conf.get(ConfigConstants.LOCAL_START_WEBSERVER));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testStop() throws Exception {
		// this only works if there is no active job at this point
		assertTrue(getRunningJobs(CLUSTER.getClusterClient()).isEmpty());

		// Create a task
		final JobVertex sender = new JobVertex("Sender");
		sender.setParallelism(2);
		sender.setInvokableClass(BlockingInvokable.class);

		final JobGraph jobGraph = new JobGraph("Stoppable streaming test job", sender);
		final JobID jid = jobGraph.getJobID();

		ClusterClient<?> clusterClient = CLUSTER.getClusterClient();
		clusterClient.setDetached(true);
		clusterClient.submitJob(jobGraph, WebFrontendITCase.class.getClassLoader());

		// wait for job to show up
		while (getRunningJobs(CLUSTER.getClusterClient()).isEmpty()) {
			Thread.sleep(10);
		}

		// wait for tasks to be properly running
		BlockingInvokable.latch.await();

		final FiniteDuration testTimeout = new FiniteDuration(2, TimeUnit.MINUTES);
		final Deadline deadline = testTimeout.fromNow();

		try (HttpTestClient client = new HttpTestClient("localhost", CLUSTER.getWebUIPort())) {
			if (CLUSTER.getCodebaseType() == TestBaseUtils.CodebaseType.NEW) {
				// stop the job
				client.sendPatchRequest("/jobs/" + jid + "/?mode=stop", deadline.timeLeft());
				HttpTestClient.SimpleHttpResponse response = client.getNextResponse(deadline.timeLeft());

				assertEquals(HttpResponseStatus.ACCEPTED, response.getStatus());
				assertEquals("application/json; charset=UTF-8", response.getType());
				assertEquals("{}", response.getContent());
			} else {
				// stop the job
				client.sendDeleteRequest("/jobs/" + jid + "/stop", deadline.timeLeft());
				HttpTestClient.SimpleHttpResponse response = client.getNextResponse(deadline.timeLeft());

				assertEquals(HttpResponseStatus.OK, response.getStatus());
				assertEquals("application/json; charset=UTF-8", response.getType());
				assertEquals("{}", response.getContent());
			}
		}

		// wait for cancellation to finish
		while (!getRunningJobs(CLUSTER.getClusterClient()).isEmpty()) {
			Thread.sleep(20);
		}

		// ensure we can access job details when its finished (FLINK-4011)
		try (HttpTestClient client = new HttpTestClient("localhost", CLUSTER.getWebUIPort())) {
			FiniteDuration timeout = new FiniteDuration(30, TimeUnit.SECONDS);
			client.sendGetRequest("/jobs/" + jid + "/config", timeout);
			HttpTestClient.SimpleHttpResponse response = client.getNextResponse(timeout);

			assertEquals(HttpResponseStatus.OK, response.getStatus());
			assertEquals("application/json; charset=UTF-8", response.getType());
			assertEquals("{\"jid\":\"" + jid + "\",\"name\":\"Stoppable streaming test job\"," +
				"\"execution-config\":{\"execution-mode\":\"PIPELINED\",\"restart-strategy\":\"Cluster level default restart strategy\"," +
				"\"job-parallelism\":-1,\"object-reuse-mode\":false,\"user-config\":{}}}", response.getContent());
		}

		BlockingInvokable.reset();
	}

	@Test
	public void testStopYarn() throws Exception {
		// this only works if there is no active job at this point
		assertTrue(getRunningJobs(CLUSTER.getClusterClient()).isEmpty());

		// Create a task
		final JobVertex sender = new JobVertex("Sender");
		sender.setParallelism(2);
		sender.setInvokableClass(BlockingInvokable.class);

		final JobGraph jobGraph = new JobGraph("Stoppable streaming test job", sender);
		final JobID jid = jobGraph.getJobID();

		ClusterClient<?> clusterClient = CLUSTER.getClusterClient();
		clusterClient.setDetached(true);
		clusterClient.submitJob(jobGraph, WebFrontendITCase.class.getClassLoader());

		// wait for job to show up
		while (getRunningJobs(CLUSTER.getClusterClient()).isEmpty()) {
			Thread.sleep(10);
		}

		// wait for tasks to be properly running
		BlockingInvokable.latch.await();

		final FiniteDuration testTimeout = new FiniteDuration(2, TimeUnit.MINUTES);
		final Deadline deadline = testTimeout.fromNow();

		try (HttpTestClient client = new HttpTestClient("localhost", CLUSTER.getWebUIPort())) {
			// Request the file from the web server
			client.sendGetRequest("/jobs/" + jid + "/yarn-stop", deadline.timeLeft());

			HttpTestClient.SimpleHttpResponse response = client
				.getNextResponse(deadline.timeLeft());

			if (CLUSTER.getCodebaseType() == TestBaseUtils.CodebaseType.NEW) {
				assertEquals(HttpResponseStatus.ACCEPTED, response.getStatus());
			} else {
				assertEquals(HttpResponseStatus.OK, response.getStatus());
			}
			assertEquals("application/json; charset=UTF-8", response.getType());
			assertEquals("{}", response.getContent());
		}

		// wait for cancellation to finish
		while (!getRunningJobs(CLUSTER.getClusterClient()).isEmpty()) {
			Thread.sleep(20);
		}

		BlockingInvokable.reset();
	}

	private static List<JobID> getRunningJobs(ClusterClient<?> client) throws Exception {
		Collection<JobStatusMessage> statusMessages = client.listJobs().get();
		return statusMessages.stream()
			.filter(status -> !status.getJobState().isGloballyTerminalState())
			.map(JobStatusMessage::getJobId)
			.collect(Collectors.toList());
	}

	/**
	 * Test invokable that is stoppable and allows waiting for all subtasks to be running.
	 */
	public static class BlockingInvokable extends AbstractInvokable implements StoppableTask {

		private static CountDownLatch latch = new CountDownLatch(2);

		private volatile boolean isRunning = true;

		public BlockingInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			latch.countDown();
			while (isRunning) {
				Thread.sleep(100);
			}
		}

		@Override
		public void stop() {
			this.isRunning = false;
		}

		public static void reset() {
			latch = new CountDownLatch(2);
		}
	}
}
