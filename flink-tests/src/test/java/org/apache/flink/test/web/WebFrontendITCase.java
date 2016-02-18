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

package org.apache.flink.test.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.commons.io.FileUtils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.testutils.StoppableInvokable;
import org.apache.flink.runtime.webmonitor.WebMonitor;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.runtime.webmonitor.files.MimeTypes;
import org.apache.flink.runtime.webmonitor.testutils.HttpTestClient;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class WebFrontendITCase extends MultipleProgramsTestBase {

	// make sure that the webserver is started for us!
	static {
		startWebServer = true;
	}

	private static int port = -1;

	@BeforeClass
	public static void initialize() {
		WebMonitor webMonitor = cluster.webMonitor().get();
		port = webMonitor.getServerPort();
	}

	public WebFrontendITCase(TestExecutionMode m) {
		super(m);
	}

	@Parameterized.Parameters(name = "Execution mode = {0}")
	public static Collection<Object[]> executionModes() {
		return Arrays.<Object[]>asList(
			new Object[] { TestExecutionMode.CLUSTER } );
	}

	@Test
	public void getFrontPage() {
		try {
			String fromHTTP = TestBaseUtils.getFromHTTP("http://localhost:" + port + "/index.html");
			String text = "Apache Flink Dashboard";
			assertTrue("Startpage should contain " + text, fromHTTP.contains(text));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
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
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void getTaskmanagers() {
		try {
			String json = getFromHTTP("http://localhost:" + port + "/taskmanagers/");

			ObjectMapper mapper = new ObjectMapper();
			JsonNode parsed = mapper.readTree(json);
			ArrayNode taskManagers = (ArrayNode) parsed.get("taskmanagers");

			assertNotNull(taskManagers);
			assertEquals(cluster.numTaskManagers(), taskManagers.size());

			JsonNode taskManager = taskManagers.get(0);
			assertNotNull(taskManager);
			assertEquals(4, taskManager.get("freeSlots").asInt());
		}
		catch(Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void getLogAndStdoutFiles() {
		try {
			WebMonitorUtils.LogFileLocation logFiles = WebMonitorUtils.LogFileLocation.find(cluster.configuration());

			FileUtils.writeStringToFile(logFiles.logFile, "job manager log");
			String logs = getFromHTTP("http://localhost:" + port + "/jobmanager/log");
			assertTrue(logs.contains("job manager log"));

			FileUtils.writeStringToFile(logFiles.stdOutFile, "job manager out");
			logs = getFromHTTP("http://localhost:" + port + "/jobmanager/stdout");
			assertTrue(logs.contains("job manager out"));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void getConfiguration() {
		try {
			String config = getFromHTTP("http://localhost:" + port + "/jobmanager/config");

			Map<String, String> conf = WebMonitorUtils.fromKeyValueJsonArray(config);
			assertTrue(conf.get(ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY).startsWith(logDir.toString()));
			assertEquals(
				cluster.configuration().getString("taskmanager.numberOfTaskSlots", null),
				conf.get(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test(timeout = 15000)
	public void testStop() throws Exception {
		// Create a task
		final JobVertex sender = new JobVertex("Sender");
		sender.setParallelism(2);
		sender.setInvokableClass(StoppableInvokable.class);

		final JobGraph jobGraph = new JobGraph("Stoppable streaming test job", sender);
		final JobID jid = jobGraph.getJobID();

		cluster.submitJobDetached(jobGraph);

		final FiniteDuration testTimeout = new FiniteDuration(2, TimeUnit.MINUTES);
		final Deadline deadline = testTimeout.fromNow();

		try (HttpTestClient client = new HttpTestClient("localhost", port)) {
			// Request the file from the web server
			client.sendDeleteRequest("/jobs/" + jid + "/stop", deadline.timeLeft());
			HttpTestClient.SimpleHttpResponse response = client.getNextResponse(deadline.timeLeft());

			assertEquals(HttpResponseStatus.OK, response.getStatus());
			assertEquals(response.getType(), MimeTypes.getMimeTypeForExtension("json"));
			assertEquals("{}", response.getContent());
		}

		waitForTaskManagers();
	}

	@Test(timeout = 15000)
	public void testStopYarn() throws Exception {
		// Create a task
		final JobVertex sender = new JobVertex("Sender");
		sender.setParallelism(2);
		sender.setInvokableClass(StoppableInvokable.class);

		final JobGraph jobGraph = new JobGraph("Stoppable streaming test job", sender);
		final JobID jid = jobGraph.getJobID();

		cluster.submitJobDetached(jobGraph);

		final FiniteDuration testTimeout = new FiniteDuration(2, TimeUnit.MINUTES);
		final Deadline deadline = testTimeout.fromNow();

		try (HttpTestClient client = new HttpTestClient("localhost", port)) {
			// Request the file from the web server
			client.sendGetRequest("/jobs/" + jid + "/yarn-stop", deadline.timeLeft());

			HttpTestClient.SimpleHttpResponse response = client
					.getNextResponse(deadline.timeLeft());

			assertEquals(HttpResponseStatus.OK, response.getStatus());
			assertEquals(response.getType(), MimeTypes.getMimeTypeForExtension("json"));
			assertEquals("{}", response.getContent());
		}

		waitForTaskManagers();
	}

	private void waitForTaskManagers() throws Exception {
		int count = 0;

		while (count != 4) {
			String json = getFromHTTP("http://localhost:" + port + "/taskmanagers/");
			JSONObject parsed = new JSONObject(json);
			JSONArray taskManagers = parsed.getJSONArray("taskmanagers");
			JSONObject taskManager = taskManagers.getJSONObject(0);
			count = taskManager.getInt("freeSlots");
		}
	}

}
