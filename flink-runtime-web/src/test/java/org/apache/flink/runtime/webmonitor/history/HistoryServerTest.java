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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.history.FsJobArchivist;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.messages.DashboardConfiguration;
import org.apache.flink.runtime.rest.messages.DashboardConfigurationHeaders;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tests for the HistoryServer.
 */
@RunWith(Parameterized.class)
public class HistoryServerTest extends TestLogger {

	@ClassRule
	public static final TemporaryFolder TMP = new TemporaryFolder();

	private static final JsonFactory JACKSON_FACTORY = new JsonFactory()
		.enable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
		.disable(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.enable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES);

	private MiniClusterWithClientResource cluster;
	private File jmDirectory;
	private File hsDirectory;

	@Parameterized.Parameters(name = "Flink version less than 1.4: {0}")
	public static Collection<Boolean> parameters() {
		return Arrays.asList(true, false);
	}

	@Parameterized.Parameter
	public static boolean versionLessThan14;

	@Before
	public void setUp() throws Exception {
		jmDirectory = TMP.newFolder("jm_" + versionLessThan14);
		hsDirectory = TMP.newFolder("hs_" + versionLessThan14);

		Configuration clusterConfig = new Configuration();
		clusterConfig.setString(JobManagerOptions.ARCHIVE_DIR, jmDirectory.toURI().toString());

		cluster = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder()
				.setConfiguration(clusterConfig)
				.setNumberTaskManagers(1)
				.setNumberSlotsPerTaskManager(1)
				.build());
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
		createLegacyArchive(jmDirectory.toPath());

		CountDownLatch numExpectedArchivedJobs = new CountDownLatch(numJobs + 1);

		Configuration historyServerConfig = new Configuration();
		historyServerConfig.setString(HistoryServerOptions.HISTORY_SERVER_ARCHIVE_DIRS, jmDirectory.toURI().toString());
		historyServerConfig.setString(HistoryServerOptions.HISTORY_SERVER_WEB_DIR, hsDirectory.getAbsolutePath());
		historyServerConfig.setLong(HistoryServerOptions.HISTORY_SERVER_ARCHIVE_REFRESH_INTERVAL, 100L);

		historyServerConfig.setInteger(HistoryServerOptions.HISTORY_SERVER_WEB_PORT, 0);

		// the job is archived asynchronously after env.execute() returns
		File[] archives = jmDirectory.listFiles();
		while (archives == null || archives.length != numJobs + 1) {
			Thread.sleep(50);
			archives = jmDirectory.listFiles();
		}

		HistoryServer hs = new HistoryServer(historyServerConfig, numExpectedArchivedJobs);
		try {
			hs.start();
			String baseUrl = "http://localhost:" + hs.getWebPort();
			numExpectedArchivedJobs.await(10L, TimeUnit.SECONDS);

			ObjectMapper mapper = new ObjectMapper();
			String response = getFromHTTP(baseUrl + JobsOverviewHeaders.URL);
			MultipleJobsDetails overview = mapper.readValue(response, MultipleJobsDetails.class);

			Assert.assertEquals(numJobs + 1, overview.getJobs().size());

			// checks whether the dashboard configuration contains all expected fields
			getDashboardConfiguration(baseUrl);
		} finally {
			hs.stop();
		}
	}

	private static DashboardConfiguration getDashboardConfiguration(String baseUrl) throws Exception {
		String response = getFromHTTP(baseUrl + DashboardConfigurationHeaders.INSTANCE.getTargetRestEndpointURL());
		return OBJECT_MAPPER.readValue(response, DashboardConfiguration.class);

	}

	private static void runJob() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.fromElements(1, 2, 3).addSink(new DiscardingSink<>());

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

	private static void createLegacyArchive(Path directory) throws IOException {
		JobID jobID = JobID.generate();

		StringWriter sw = new StringWriter();
		try (JsonGenerator gen = JACKSON_FACTORY.createGenerator(sw)) {
			try (JsonObject root = new JsonObject(gen)) {
				try (JsonArray finished = new JsonArray(gen, "finished")) {
					try (JsonObject job = new JsonObject(gen)) {
						gen.writeStringField("jid", jobID.toString());
						gen.writeStringField("name", "testjob");
						gen.writeStringField("state", JobStatus.FINISHED.name());

						gen.writeNumberField("start-time", 0L);
						gen.writeNumberField("end-time", 1L);
						gen.writeNumberField("duration", 1L);
						gen.writeNumberField("last-modification", 1L);

						try (JsonObject tasks = new JsonObject(gen, "tasks")) {
							gen.writeNumberField("total", 0);

							if (versionLessThan14) {
								gen.writeNumberField("pending", 0);
							} else {
								gen.writeNumberField("created", 0);
								gen.writeNumberField("deploying", 0);
								gen.writeNumberField("scheduled", 0);
							}
							gen.writeNumberField("running", 0);
							gen.writeNumberField("finished", 0);
							gen.writeNumberField("canceling", 0);
							gen.writeNumberField("canceled", 0);
							gen.writeNumberField("failed", 0);
						}
					}
				}
			}
		}
		String json = sw.toString();

		ArchivedJson archivedJson = new ArchivedJson("/joboverview", json);

		FsJobArchivist.archiveJob(new org.apache.flink.core.fs.Path(directory.toUri()), jobID, Collections.singleton(archivedJson));
	}

	private static final class JsonObject implements AutoCloseable {

		private final JsonGenerator gen;

		JsonObject(JsonGenerator gen) throws IOException {
			this.gen = gen;
			gen.writeStartObject();
		}

		private JsonObject(JsonGenerator gen, String name) throws IOException {
			this.gen = gen;
			gen.writeObjectFieldStart(name);
		}

		@Override
		public void close() throws IOException {
			gen.writeEndObject();
		}
	}

	private static final class JsonArray implements AutoCloseable {

		private final JsonGenerator gen;

		JsonArray(JsonGenerator gen, String name) throws IOException {
			this.gen = gen;
			gen.writeArrayFieldStart(name);
		}

		@Override
		public void close() throws IOException {
			gen.writeEndArray();
		}
	}
}
