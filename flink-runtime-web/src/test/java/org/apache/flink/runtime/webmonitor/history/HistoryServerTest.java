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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.history.FsJobArchivist;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.category.AlsoRunWithSchedulerNG;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
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
@Category(AlsoRunWithSchedulerNG.class)
public class HistoryServerTest extends TestLogger {

	private static final JsonFactory JACKSON_FACTORY = new JsonFactory()
		.enable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
		.disable(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

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
		jmDirectory = tmpFolder.newFolder("jm_" + versionLessThan14);
		hsDirectory = tmpFolder.newFolder("hs_" + versionLessThan14);

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
		final int numLegacyJobs = 1;
		createLegacyArchive(jmDirectory.toPath());

		waitForArchivesCreation(numJobs + numLegacyJobs);

		CountDownLatch numExpectedArchivedJobs = new CountDownLatch(numJobs + numLegacyJobs);

		Configuration historyServerConfig = createTestConfiguration(false);

		HistoryServer hs = new HistoryServer(historyServerConfig, (event) -> {
			if (event.getType() == HistoryServerArchiveFetcher.ArchiveEventType.CREATED) {
				numExpectedArchivedJobs.countDown();
			}
		});

		try {
			hs.start();
			String baseUrl = "http://localhost:" + hs.getWebPort();
			numExpectedArchivedJobs.await(10L, TimeUnit.SECONDS);

			Assert.assertEquals(numJobs + numLegacyJobs, getJobsOverview(baseUrl).getJobs().size());
		} finally {
			hs.stop();
		}
	}

	@Test
	public void testCleanExpiredJob() throws Exception {
		runArchiveExpirationTest(true);
	}

	@Test
	public void testRemainExpiredJob() throws Exception {
		runArchiveExpirationTest(false);
	}

	private void runArchiveExpirationTest(boolean cleanupExpiredJobs) throws Exception {
		int numExpiredJobs = cleanupExpiredJobs ? 1 : 0;
		int numJobs = 3;
		for (int x = 0; x < numJobs; x++) {
			runJob();
		}
		waitForArchivesCreation(numJobs);

		CountDownLatch numExpectedArchivedJobs = new CountDownLatch(numJobs);
		CountDownLatch numExpectedExpiredJobs = new CountDownLatch(numExpiredJobs);

		Configuration historyServerConfig = createTestConfiguration(cleanupExpiredJobs);

		HistoryServer hs =
			new HistoryServer(
				historyServerConfig,
				(event) -> {
					switch (event.getType()){
						case CREATED:
							numExpectedArchivedJobs.countDown();
							break;
						case DELETED:
							numExpectedExpiredJobs.countDown();
							break;
					}
				});

		try {
			hs.start();
			String baseUrl = "http://localhost:" + hs.getWebPort();
			numExpectedArchivedJobs.await(10L, TimeUnit.SECONDS);

			Collection<JobDetails> jobs = getJobsOverview(baseUrl).getJobs();
			Assert.assertEquals(numJobs, jobs.size());

			String jobIdToDelete = jobs.stream()
				.findFirst()
				.map(JobDetails::getJobId)
				.map(JobID::toString)
				.orElseThrow(() -> new IllegalStateException("Expected at least one existing job"));

			// delete one archive from jm
			Files.deleteIfExists(jmDirectory.toPath().resolve(jobIdToDelete));

			numExpectedExpiredJobs.await(10L, TimeUnit.SECONDS);

			// check that archive is present in hs
			Collection<JobDetails> jobsAfterDeletion = getJobsOverview(baseUrl).getJobs();
			Assert.assertEquals(numJobs - numExpiredJobs, jobsAfterDeletion.size());
			Assert.assertEquals(1 - numExpiredJobs, jobsAfterDeletion.stream()
				.map(JobDetails::getJobId)
				.map(JobID::toString)
				.filter(jobId -> jobId.equals(jobIdToDelete))
				.count());
		} finally {
			hs.stop();
		}
	}

	private void waitForArchivesCreation(int numJobs) throws InterruptedException {
		// the job is archived asynchronously after env.execute() returns
		File[] archives = jmDirectory.listFiles();
		while (archives == null || archives.length != numJobs) {
			Thread.sleep(50);
			archives = jmDirectory.listFiles();
		}
	}

	private Configuration createTestConfiguration(boolean cleanupExpiredJobs) {
		Configuration historyServerConfig = new Configuration();
		historyServerConfig.setString(HistoryServerOptions.HISTORY_SERVER_ARCHIVE_DIRS, jmDirectory.toURI().toString());
		historyServerConfig.setString(HistoryServerOptions.HISTORY_SERVER_WEB_DIR, hsDirectory.getAbsolutePath());
		historyServerConfig.setLong(HistoryServerOptions.HISTORY_SERVER_ARCHIVE_REFRESH_INTERVAL, 100L);

		historyServerConfig.setBoolean(HistoryServerOptions.HISTORY_SERVER_CLEANUP_EXPIRED_JOBS, cleanupExpiredJobs);

		historyServerConfig.setInteger(HistoryServerOptions.HISTORY_SERVER_WEB_PORT, 0);
		return historyServerConfig;
	}

	private static MultipleJobsDetails getJobsOverview(String baseUrl) throws Exception {
		String response = getFromHTTP(baseUrl + JobsOverviewHeaders.URL);
		return OBJECT_MAPPER.readValue(response, MultipleJobsDetails.class);
	}

	private static void runJob() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.fromElements(1, 2, 3).addSink(new DiscardingSink<>());

		env.execute();
	}

	static String getFromHTTP(String url) throws Exception {
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

	private static String createLegacyArchive(Path directory) throws IOException {
		JobID jobId = JobID.generate();

		StringWriter sw = new StringWriter();
		try (JsonGenerator gen = JACKSON_FACTORY.createGenerator(sw)) {
			try (JsonObject root = new JsonObject(gen)) {
				try (JsonArray finished = new JsonArray(gen, "finished")) {
					try (JsonObject job = new JsonObject(gen)) {
						gen.writeStringField("jid", jobId.toString());
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

		FsJobArchivist.archiveJob(new org.apache.flink.core.fs.Path(directory.toUri()), jobId, Collections.singleton(archivedJson));

		return jobId.toString();
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
