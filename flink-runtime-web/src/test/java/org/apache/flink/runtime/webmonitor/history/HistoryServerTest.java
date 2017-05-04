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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.messages.ArchiveMessages;
import org.apache.flink.runtime.webmonitor.utils.ArchivedJobGenerationUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import scala.Option;

public class HistoryServerTest {

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

		ActorSystem actorSystem = AkkaUtils.createLocalActorSystem(config);
		Option<Path> archivePath = Option.apply(new Path(jmDirectory.toURI().toString()));

		ActorRef memoryArchivist = actorSystem.actorOf(JobManager.getArchiveProps(MemoryArchivist.class, 1, archivePath));
		memoryArchivist.tell(new ArchiveMessages.ArchiveExecutionGraph(graph.getJobID(), graph), null);

		File archive = new File(jmDirectory, graph.getJobID().toString());
		for (int x = 0; x < 10 && !archive.exists(); x++) {
			Thread.sleep(50);
		}
		Assert.assertTrue(archive.exists());

		HistoryServer hs = new HistoryServer(config);
		try {
			hs.start();
			ObjectMapper mapper = new ObjectMapper();
			JsonNode overview = null;
			for (int x = 0; x < 20; x++) {
				Thread.sleep(50);
				String response = getFromHTTP("http://localhost:8082/joboverview");
				if (response.contains("404 Not Found")) {
					// file may not be written yet
					continue;
				} else {
					try {
						overview = mapper.readTree(response);
						break;
					} catch (IOException ignored) {
						// while the file may exist the contents may not have been written yet
						continue;
					}
				}
			}
			Assert.assertNotNull("/joboverview.json did not contain valid json", overview);
			String jobID = overview.get("finished").get(0).get("jid").asText();
			JsonNode jobDetails = mapper.readTree(getFromHTTP("http://localhost:8082/jobs/" + jobID));
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
