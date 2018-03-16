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
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.messages.ArchiveMessages;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedJobGenerationUtils;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import scala.Option;

/**
 * Tests for the HistoryServer.
 */
public class HistoryServerTest extends TestLogger {

	@Rule
	public TemporaryFolder tmpDir = new TemporaryFolder();

	@Test
	public void testFullArchiveLifecycle() throws Exception {
		ArchivedExecutionGraph graph = (ArchivedExecutionGraph) ArchivedJobGenerationUtils.getTestJob();

		File jmDirectory = tmpDir.newFolder("jm");
		File hsDirectory = tmpDir.newFolder("hs");

		Configuration config = new Configuration();
		config.setString(JobManagerOptions.ARCHIVE_DIR, jmDirectory.toURI().toString());

		config.setString(HistoryServerOptions.HISTORY_SERVER_ARCHIVE_DIRS, jmDirectory.toURI().toString());
		config.setString(HistoryServerOptions.HISTORY_SERVER_WEB_DIR, hsDirectory.getAbsolutePath());

		config.setInteger(HistoryServerOptions.HISTORY_SERVER_WEB_PORT, 0);

		ActorSystem actorSystem = AkkaUtils.createLocalActorSystem(config);
		Option<Path> archivePath = Option.apply(new Path(jmDirectory.toURI().toString()));

		ActorRef memoryArchivist = TestActorRef.apply(JobManager.getArchiveProps(MemoryArchivist.class, 1, archivePath), actorSystem);
		memoryArchivist.tell(new ArchiveMessages.ArchiveExecutionGraph(graph.getJobID(), graph), null);

		File archive = new File(jmDirectory, graph.getJobID().toString());
		Assert.assertTrue(archive.exists());

		CountDownLatch numFinishedPolls = new CountDownLatch(1);

		HistoryServer hs = new HistoryServer(config, numFinishedPolls);
		try {
			hs.start();
			String baseUrl = "http://localhost:" + hs.getWebPort();
			numFinishedPolls.await(10L, TimeUnit.SECONDS);

			ObjectMapper mapper = new ObjectMapper();
			String response = getFromHTTP(baseUrl + JobsOverviewHeaders.URL);
			JsonNode overview = mapper.readTree(response);

			String jobID = overview.get("jobs").get(0).get("jid").asText();
			JsonNode jobDetails = mapper.readTree(getFromHTTP(baseUrl + "/jobs/" + jobID));
			Assert.assertNotNull(jobDetails.get("jid"));
		} finally {
			hs.stop();
		}
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
