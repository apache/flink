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


import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.webmonitor.WebMonitor;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.test.util.TestBaseUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

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
	public static Collection<TestExecutionMode[]> executionModes(){
		Collection<TestExecutionMode[]> c = new ArrayList<TestExecutionMode[]>(1);
		c.add(new TestExecutionMode[] {TestExecutionMode.CLUSTER});
		return c;
	}

	@Test
	public void getFrontPage() {
		try {
			String fromHTTP = TestBaseUtils.getFromHTTP("http://localhost:" + port + "/index.html");
			String text = "Apache Flink Dashboard";
			Assert.assertTrue("Startpage should contain " + text, fromHTTP.contains(text));
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void getNumberOfTaskManagers() {
		try {
			String json = TestBaseUtils.getFromHTTP("http://localhost:" + port + "/taskmanagers/");
			JSONObject response = new JSONObject(json);
			JSONArray taskManagers = response.getJSONArray("taskmanagers");
			Assert.assertNotNull(taskManagers);
			Assert.assertEquals(cluster.numTaskManagers(), taskManagers.length());
		}catch(Throwable e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void getTaskmanagers() {
		try {
			String json = getFromHTTP("http://localhost:" + port + "/taskmanagers/");
			JSONObject parsed = new JSONObject(json);
			JSONArray taskManagers = parsed.getJSONArray("taskmanagers");
			Assert.assertNotNull(taskManagers);
			Assert.assertEquals(cluster.numTaskManagers(), taskManagers.length());
			JSONObject taskManager = taskManagers.getJSONObject(0);
			Assert.assertNotNull(taskManager);
			Assert.assertEquals(4, taskManager.getInt("freeSlots"));
		}catch(Throwable e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void getLogAndStdoutFiles() {
		try {
			WebMonitorUtils.LogFileLocation logFiles = WebMonitorUtils.LogFileLocation.find(cluster.configuration());

			FileUtils.writeStringToFile(logFiles.logFile, "job manager log");
			String logs = getFromHTTP("http://localhost:" + port + "/jobmanager/log");
			Assert.assertTrue(logs.contains("job manager log"));

			FileUtils.writeStringToFile(logFiles.stdOutFile, "job manager out");
			logs = getFromHTTP("http://localhost:" + port + "/jobmanager/stdout");
			Assert.assertTrue(logs.contains("job manager out"));
		} catch(Throwable e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void getConfiguration() {
		try {
			String config = getFromHTTP("http://localhost:" + port + "/jobmanager/config");
			JSONArray array = new JSONArray(config);

			Map<String, String> conf = WebMonitorUtils.fromKeyValueJsonArray(array);
			Assert.assertTrue(conf.get(ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY).startsWith(logDir.toString()));
			Assert.assertEquals(
					cluster.configuration().getString("taskmanager.numberOfTaskSlots", null),
					conf.get(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS));
		} catch (Throwable e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

}
