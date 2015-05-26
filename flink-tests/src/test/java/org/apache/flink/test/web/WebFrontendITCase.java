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
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.test.util.TestBaseUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

@RunWith(Parameterized.class)
public class WebFrontendITCase extends MultipleProgramsTestBase {

	@BeforeClass
	public static void setup() throws Exception{
		cluster = TestBaseUtils.startCluster(1, 4, true, true);
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
	public void getNumberOfTaskManagers() {
		try {
			Assert.assertEquals("{\"taskmanagers\": "+cluster.getTaskManagers().size()+", \"slots\": 4}", TestBaseUtils.getFromHTTP("http://localhost:8081/jobsInfo?get=taskmanagers"));
		}catch(Throwable e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void getTaskmanagers() {
		try {
			String json = getFromHTTP("http://localhost:8081/setupInfo?get=taskmanagers");
			JSONObject parsed = new JSONObject(json);
			Object taskManagers = parsed.get("taskmanagers");
			Assert.assertNotNull(taskManagers);
			Assert.assertTrue(taskManagers instanceof JSONArray);
			JSONArray tma = (JSONArray) taskManagers;
			Assert.assertEquals(cluster.numTaskManagers(), tma.length());
			Object taskManager = tma.get(0);
			Assert.assertNotNull(taskManager);
			Assert.assertTrue(taskManager instanceof JSONObject);
			Assert.assertEquals(4, ((JSONObject) taskManager).getInt("freeSlots"));
		}catch(Throwable e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void getLogfiles() {
		try {
			String logPath = cluster.configuration().getString(ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY, null);
			Assert.assertNotNull(logPath);
			FileUtils.writeStringToFile(new File(logPath, "jobmanager-main.log"), "test content");

			String logs = getFromHTTP("http://localhost:8081/logInfo");
			Assert.assertTrue(logs.contains("test content"));
		}catch(Throwable e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void getConfiguration() {
		try {
			String config = getFromHTTP("http://localhost:8081/setupInfo?get=globalC");
			JSONObject parsed = new JSONObject(config);
			Assert.assertEquals(logDir.toString(), parsed.getString("jobmanager.web.logpath"));
			Assert.assertEquals(cluster.configuration().getString("taskmanager.numberOfTaskSlots", null), parsed.getString("taskmanager.numberOfTaskSlots"));
		}catch(Throwable e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

}
