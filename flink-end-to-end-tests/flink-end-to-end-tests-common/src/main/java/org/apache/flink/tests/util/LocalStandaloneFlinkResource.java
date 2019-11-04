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

package org.apache.flink.tests.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Flink resource to manage the local standalone flink cluster, such as setUp, start, stop clusters and so on.
 */
public class LocalStandaloneFlinkResource implements FlinkResource {
	private static final Logger LOG = LoggerFactory.getLogger(LocalStandaloneFlinkResource.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private final Path conf;
	private final Path bin;

	public LocalStandaloneFlinkResource(String distDirProperty) {
		final Path flinkDir = Paths.get(distDirProperty);
		this.bin = flinkDir.resolve("bin");
		this.conf = flinkDir.resolve("conf");
	}

	/**
	 * Read the value of `rest.port` part in <distDir>/conf/flink-conf.yaml.
	 *
	 * @return the rest port which standalone Flink cluster will listen.
	 */
	private int getRestPort() {
		Configuration config = GlobalConfiguration.loadConfiguration(conf.toAbsolutePath().toString());
		return config.getInteger("rest.port", 8081);
	}

	@Override
	public void startCluster(int taskManagerNumber) throws IOException {
		LOG.info("Starting Flink cluster.");
		for (int i = 0; i < taskManagerNumber; i++) {
			AutoClosableProcess.runBlocking(bin.resolve("start-cluster.sh").toAbsolutePath().toString());
		}

		final OkHttpClient client = new OkHttpClient();
		final Request request = new Request.Builder()
			.get()
			.url("http://localhost:" + getRestPort() + "/taskmanagers")
			.build();

		Exception reportedException = null;
		for (int retryAttempt = 0; retryAttempt < 30; retryAttempt++) {
			try (Response response = client.newCall(request).execute()) {
				if (response.isSuccessful()) {
					final String json = response.body().string();
					final JsonNode taskManagerList = OBJECT_MAPPER.readTree(json)
						.get("taskmanagers");

					if (taskManagerList != null && taskManagerList.size() >= taskManagerNumber) {
						LOG.info("Dispatcher REST endpoint is up.");
						return;
					}
				}
			} catch (IOException ioe) {
				reportedException = ExceptionUtils.firstOrSuppressed(ioe, reportedException);
			}

			LOG.info("Waiting for dispatcher REST endpoint to come up...");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				reportedException = ExceptionUtils.firstOrSuppressed(e, reportedException);
			}
		}
		throw new AssertionError("Dispatcher REST endpoint did not start in time.", reportedException);
	}

	@Override
	public void stopJobManager() throws IOException {
		LOG.info("Stopping the job managers");
		AutoClosableProcess.killJavaProcess("StandaloneSessionClusterEntrypoint", false);
	}

	@Override
	public void stopTaskMangers() throws IOException {
		LOG.info("Stopping all task managers.");
		AutoClosableProcess.killJavaProcess("TaskManagerRunner", false);
	}

	@Override
	public void stopCluster() throws IOException {
		LOG.info("Stopping Flink cluster, both the job manager and task managers will be stopped.");
		stopJobManager();
		stopTaskMangers();
	}

	@Override
	public FlinkClient createFlinkClient() throws IOException {
		return new FlinkClient(this.bin);
	}

	@Override
	public FlinkSQLClient createFlinkSQLClient() throws IOException {
		return new FlinkSQLClient(this.bin);
	}
}
