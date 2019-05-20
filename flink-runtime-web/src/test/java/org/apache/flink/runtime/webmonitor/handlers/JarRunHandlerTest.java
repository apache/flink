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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link JarRunHandler}.
 */
public class JarRunHandlerTest extends TestLogger {

	@ClassRule
	public static final TemporaryFolder TMP = new TemporaryFolder();

	@Test
	public void testRunJar() throws Exception {
		Path uploadDir = TMP.newFolder().toPath();

		Path actualUploadDir = uploadDir.resolve("flink-web-upload");
		Files.createDirectory(actualUploadDir);

		Path emptyJar = actualUploadDir.resolve("empty.jar");
		Files.createFile(emptyJar);

		Configuration config = new Configuration();
		config.setString(WebOptions.UPLOAD_DIR, uploadDir.toString());

		MiniClusterResource clusterResource = new MiniClusterResource(
			new MiniClusterResourceConfiguration.Builder()
				.setConfiguration(config)
				.setNumberTaskManagers(1)
				.setNumberSlotsPerTaskManager(1)
				.build());
		clusterResource.before();

		try {
			Configuration clientConfig = clusterResource.getClientConfiguration();
			RestClient client = new RestClient(RestClientConfiguration.fromConfiguration(clientConfig), TestingUtils.defaultExecutor());

			try {
				JarRunHeaders headers = JarRunHeaders.getInstance();
				JarRunMessageParameters parameters = headers.getUnresolvedMessageParameters();
				parameters.jarIdPathParameter.resolve(emptyJar.getFileName().toString());

				String host = clientConfig.getString(RestOptions.ADDRESS);
				int port = clientConfig.getInteger(RestOptions.PORT);

				try {
					client.sendRequest(host, port, headers, parameters, new JarRunRequestBody())
						.get();
				} catch (Exception e) {
					Optional<RestClientException> expected = ExceptionUtils.findThrowable(e, RestClientException.class);
					if (expected.isPresent()) {
						// implies the job was actually submitted
						assertTrue(expected.get().getMessage().contains("ProgramInvocationException"));
						// original cause is preserved in stack trace
						assertThat(expected.get().getMessage(), containsString("ZipException"));
						// implies the jar was registered for the job graph (otherwise the jar name would not occur in the exception)
						// implies the jar was uploaded (otherwise the file would not be found at all)
						assertTrue(expected.get().getMessage().contains("empty.jar'. zip file is empty"));
					} else {
						throw e;
					}
				}
			} finally {
				client.shutdown(Time.milliseconds(10));
			}
		} finally {
			clusterResource.after();
		}
	}
}
