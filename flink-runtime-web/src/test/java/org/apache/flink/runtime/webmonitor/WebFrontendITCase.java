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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.runtime.testutils.StoppableInvokable;
import org.apache.flink.runtime.webmonitor.testutils.HttpTestClient;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

	private static LocalFlinkMiniCluster cluster;

	private static int port = -1;

	@BeforeClass
	public static void initialize() throws Exception {
		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TASK_MANAGERS);
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, NUM_SLOTS);
		config.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 12L);
		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

		File logDir = File.createTempFile("TestBaseUtils-logdir", null);
		assertTrue("Unable to delete temp file", logDir.delete());
		assertTrue("Unable to create temp directory", logDir.mkdir());
		File logFile = new File(logDir, "jobmanager.log");
		File outFile = new File(logDir, "jobmanager.out");

		Files.createFile(logFile.toPath());
		Files.createFile(outFile.toPath());

		config.setString(WebOptions.LOG_PATH, logFile.getAbsolutePath());
		config.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, logFile.getAbsolutePath());

		cluster = new LocalFlinkMiniCluster(config, false);
		cluster.start();

		port = cluster.webMonitor().get().getServerPort();
	}

	@Test
	public void getFrontPage() {
		try {
			String fromHTTP = TestBaseUtils.getFromHTTP("http://localhost:" + port + "/index.html");
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
		URL taskManagersUrl = new URL("http://localhost:" + port + "/taskmanagers");
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
		URL notFoundJobUrl = new URL("http://localhost:" + port + "/jobs/dontexist");
		HttpURLConnection notFoundJobConnection = (HttpURLConnection) notFoundJobUrl.openConnection();
		notFoundJobConnection.setConnectTimeout(100000);
		notFoundJobConnection.connect();
		if (notFoundJobConnection.getResponseCode() >= 400) {
			// we don't set the content-encoding header
			Assert.assertNull(notFoundJobConnection.getContentEncoding());
			Assert.assertEquals("text/plain; charset=UTF-8", notFoundJobConnection.getContentType());
		} else {
			throw new RuntimeException("Request for non-existing job did not return an error.");
		}
	}

	@Test
	public void getNumberOfTaskManagers() {
		try {
			String json = TestBaseUtils.getFromHTTP("http://localhost:" + port + "/taskmanagers/");

			ObjectMapper mapper = new ObjectMapper();
			JsonNode response = mapper.readTree(json);
			ArrayNode taskManagers = (ArrayNode) response.get("taskmanagers");

			assertNotNull(taskManagers);
			assertEquals(cluster.numTaskManagers(), taskManagers.size());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void getTaskmanagers() throws Exception {
		String json = TestBaseUtils.getFromHTTP("http://localhost:" + port + "/taskmanagers/");

		ObjectMapper mapper = new ObjectMapper();
		JsonNode parsed = mapper.readTree(json);
		ArrayNode taskManagers = (ArrayNode) parsed.get("taskmanagers");

		assertNotNull(taskManagers);
		assertEquals(cluster.numTaskManagers(), taskManagers.size());

		JsonNode taskManager = taskManagers.get(0);
		assertNotNull(taskManager);
		assertEquals(NUM_SLOTS, taskManager.get("slotsNumber").asInt());
		assertTrue(taskManager.get("freeSlots").asInt() <= NUM_SLOTS);
	}

	@Test
	public void getLogAndStdoutFiles() throws Exception {
		WebMonitorUtils.LogFileLocation logFiles = WebMonitorUtils.LogFileLocation.find(cluster.configuration());

		FileUtils.writeStringToFile(logFiles.logFile, "job manager log");
		String logs = TestBaseUtils.getFromHTTP("http://localhost:" + port + "/jobmanager/log");
		assertTrue(logs.contains("job manager log"));

		FileUtils.writeStringToFile(logFiles.stdOutFile, "job manager out");
		logs = TestBaseUtils.getFromHTTP("http://localhost:" + port + "/jobmanager/stdout");
		assertTrue(logs.contains("job manager out"));
	}

	@Test
	public void getTaskManagerLogAndStdoutFiles() {
		try {
			String json = TestBaseUtils.getFromHTTP("http://localhost:" + port + "/taskmanagers/");

			ObjectMapper mapper = new ObjectMapper();
			JsonNode parsed = mapper.readTree(json);
			ArrayNode taskManagers = (ArrayNode) parsed.get("taskmanagers");
			JsonNode taskManager = taskManagers.get(0);
			String id = taskManager.get("id").asText();

			WebMonitorUtils.LogFileLocation logFiles = WebMonitorUtils.LogFileLocation.find(cluster.configuration());

			//we check for job manager log files, since no separate taskmanager logs exist
			FileUtils.writeStringToFile(logFiles.logFile, "job manager log");
			String logs = TestBaseUtils.getFromHTTP("http://localhost:" + port + "/taskmanagers/" + id + "/log");
			assertTrue(logs.contains("job manager log"));

			FileUtils.writeStringToFile(logFiles.stdOutFile, "job manager out");
			logs = TestBaseUtils.getFromHTTP("http://localhost:" + port + "/taskmanagers/" + id + "/stdout");
			assertTrue(logs.contains("job manager out"));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void getConfiguration() {
		try {
			String config = TestBaseUtils.getFromHTTP("http://localhost:" + port + "/jobmanager/config");

			Map<String, String> conf = WebMonitorUtils.fromKeyValueJsonArray(config);
			assertEquals(
				cluster.configuration().getString("taskmanager.numberOfTaskSlots", null),
				conf.get(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testStop() throws Exception {
		// this only works if there is no active job at this point
		assertTrue(cluster.getCurrentlyRunningJobsJava().isEmpty());

		// Create a task
		final JobVertex sender = new JobVertex("Sender");
		sender.setParallelism(2);
		sender.setInvokableClass(StoppableInvokable.class);

		final JobGraph jobGraph = new JobGraph("Stoppable streaming test job", sender);
		final JobID jid = jobGraph.getJobID();

		cluster.submitJobDetached(jobGraph);

		// wait for job to show up
		while (cluster.getCurrentlyRunningJobsJava().isEmpty()) {
			Thread.sleep(10);
		}

		final FiniteDuration testTimeout = new FiniteDuration(2, TimeUnit.MINUTES);
		final Deadline deadline = testTimeout.fromNow();

		while (!cluster.getCurrentlyRunningJobsJava().isEmpty()) {
			try (HttpTestClient client = new HttpTestClient("localhost", port)) {
				// Request the file from the web server
				client.sendDeleteRequest("/jobs/" + jid + "/stop", deadline.timeLeft());
				HttpTestClient.SimpleHttpResponse response = client.getNextResponse(deadline.timeLeft());

				assertEquals(HttpResponseStatus.OK, response.getStatus());
				assertEquals("application/json; charset=UTF-8", response.getType());
				assertEquals("{}", response.getContent());
			}

			Thread.sleep(20);
		}

		// ensure we can access job details when its finished (FLINK-4011)
		try (HttpTestClient client = new HttpTestClient("localhost", port)) {
			FiniteDuration timeout = new FiniteDuration(30, TimeUnit.SECONDS);
			client.sendGetRequest("/jobs/" + jid + "/config", timeout);
			HttpTestClient.SimpleHttpResponse response = client.getNextResponse(timeout);

			assertEquals(HttpResponseStatus.OK, response.getStatus());
			assertEquals("application/json; charset=UTF-8", response.getType());
			assertEquals("{\"jid\":\"" + jid + "\",\"name\":\"Stoppable streaming test job\"," +
				"\"execution-config\":{\"execution-mode\":\"PIPELINED\",\"restart-strategy\":\"default\"," +
				"\"job-parallelism\":-1,\"object-reuse-mode\":false,\"user-config\":{}}}", response.getContent());
		}
	}

	@Test
	public void testStopYarn() throws Exception {
		// this only works if there is no active job at this point
		assertTrue(cluster.getCurrentlyRunningJobsJava().isEmpty());

		// Create a task
		final JobVertex sender = new JobVertex("Sender");
		sender.setParallelism(2);
		sender.setInvokableClass(StoppableInvokable.class);

		final JobGraph jobGraph = new JobGraph("Stoppable streaming test job", sender);
		final JobID jid = jobGraph.getJobID();

		cluster.submitJobDetached(jobGraph);

		// wait for job to show up
		while (cluster.getCurrentlyRunningJobsJava().isEmpty()) {
			Thread.sleep(10);
		}

		final FiniteDuration testTimeout = new FiniteDuration(2, TimeUnit.MINUTES);
		final Deadline deadline = testTimeout.fromNow();

		while (!cluster.getCurrentlyRunningJobsJava().isEmpty()) {
			try (HttpTestClient client = new HttpTestClient("localhost", port)) {
				// Request the file from the web server
				client.sendGetRequest("/jobs/" + jid + "/yarn-stop", deadline.timeLeft());

				HttpTestClient.SimpleHttpResponse response = client
					.getNextResponse(deadline.timeLeft());

				assertEquals(HttpResponseStatus.OK, response.getStatus());
				assertEquals("application/json; charset=UTF-8", response.getType());
				assertEquals("{}", response.getContent());
			}

			Thread.sleep(20);
		}
	}
}
