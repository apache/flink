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

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tests for the HistoryServer.
 */
public class HistoryServerTest extends TestLogger {

	@ClassRule
	public static final TemporaryFolder TMP = new TemporaryFolder();

	private MiniClusterResource cluster;
	private File jmDirectory;
	private File hsDirectory;

	@Before
	public void setUp() throws Exception {
		jmDirectory = TMP.newFolder("jm");
		hsDirectory = TMP.newFolder("hs");

		Configuration clusterConfig = new Configuration();
		clusterConfig.setString(JobManagerOptions.ARCHIVE_DIR, jmDirectory.toURI().toString());

		cluster = new MiniClusterResource(
			new MiniClusterResource.MiniClusterResourceConfiguration(
				clusterConfig,
				1,
				1
			),
			MiniClusterResource.MiniClusterType.NEW
		);
		cluster.before();
	}

	@After
	public void tearDown() {
		if (cluster != null) {
			cluster.after();
		}
	}

	@Test
	public void testHistoryServerIntegration() throws Exception {
		final int numJobs = 2;
		for (int x = 0; x < numJobs; x++) {
			runJob();
		}

		CountDownLatch numFinishedPolls = new CountDownLatch(1);

		Configuration historyServerConfig = new Configuration();
		historyServerConfig.setString(HistoryServerOptions.HISTORY_SERVER_ARCHIVE_DIRS, jmDirectory.toURI().toString());
		historyServerConfig.setString(HistoryServerOptions.HISTORY_SERVER_WEB_DIR, hsDirectory.getAbsolutePath());

		historyServerConfig.setInteger(HistoryServerOptions.HISTORY_SERVER_WEB_PORT, 0);

		// the job is archived asynchronously after env.execute() returns
		File[] archives = jmDirectory.listFiles();
		while (archives == null || archives.length != numJobs) {
			Thread.sleep(50);
			archives = jmDirectory.listFiles();
		}

		HistoryServer hs = new HistoryServer(historyServerConfig, numFinishedPolls);
		try {
			hs.start();
			String baseUrl = "http://localhost:" + hs.getWebPort();
			numFinishedPolls.await(10L, TimeUnit.SECONDS);

			ObjectMapper mapper = new ObjectMapper();
			String response = getFromHTTP(baseUrl + JobsOverviewHeaders.URL);
			MultipleJobsDetails overview = mapper.readValue(response, MultipleJobsDetails.class);

			Assert.assertEquals(numJobs, overview.getJobs().size());
		} finally {
			hs.stop();
		}
	}

	private static void runJob() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.fromElements(1, 2, 3)
			.print();

		env.execute();
	}

	public static String getFromHTTP(String url) throws Exception {
		URL u = new URL(url);
		HttpURLConnection connection = (HttpURLConnection) u.openConnection();
		connection.setConnectTimeout(100000);
		connection.connect();
		InputStream is;
		if (connection.getResponseCode() >= 400) {
			// error!
			is = connection.getErrorStream();
		} else {
			is = connection.getInputStream();
		}

		return IOUtils.toString(is, connection.getContentEncoding() != null ? connection.getContentEncoding() : "UTF-8");
	}
}
