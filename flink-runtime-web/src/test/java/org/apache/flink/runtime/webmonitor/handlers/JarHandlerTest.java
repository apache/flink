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
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.testutils.junit.category.AlsoRunWithLegacyScheduler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for the {@link JarRunHandler} and  {@link JarPlanHandler}.
 */
@Category(AlsoRunWithLegacyScheduler.class)
public class JarHandlerTest extends TestLogger {

	@ClassRule
	public static final TemporaryFolder TMP = new TemporaryFolder();

	enum Type {
		PLAN,
		RUN
	}

	@Test
	public void testPlanJar() throws Exception {
		runTest(Type.PLAN, "hello out!", "hello err!");
	}

	@Test
	public void testRunJar() throws Exception {
		runTest(Type.RUN, "(none)", "(none)");
	}

	private static void runTest(Type type, String expectedCapturedStdOut, String expectedCapturedStdErr) throws Exception {
		Path uploadDir = TMP.newFolder().toPath();

		Path actualUploadDir = uploadDir.resolve("flink-web-upload");
		Files.createDirectory(actualUploadDir);

		Path emptyJar = actualUploadDir.resolve("empty.jar");
		createJarFile(emptyJar);

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
				final MessageHeaders headers;
				final JarMessageParameters parameters;
				if (type == Type.RUN) {
					headers = JarRunHeaders.getInstance();
					parameters = ((JarRunHeaders) headers).getUnresolvedMessageParameters();
				} else if (type == Type.PLAN) {
					headers = JarPlanGetHeaders.getInstance();
					parameters = ((JarPlanGetHeaders) headers).getUnresolvedMessageParameters();
				} else {
					throw new RuntimeException("Invalid type: " + type);
				}
				parameters.jarIdPathParameter.resolve(emptyJar.getFileName().toString());

				String host = clientConfig.getString(RestOptions.ADDRESS);
				int port = clientConfig.getInteger(RestOptions.PORT);

				try {
					client.sendRequest(host, port, headers, parameters, new JarPlanRequestBody()).get();
				} catch (Exception e) {
					Optional<RestClientException> expected = ExceptionUtils.findThrowable(e, RestClientException.class);
					if (expected.isPresent()) {
						String message = expected.get().getMessage();
						// implies the job was actually submitted
						assertThat(message, containsString("ProgramInvocationException"));
						// original cause is preserved in stack trace
						assertThat(message, containsString("The program plan could not be fetched - the program aborted pre-maturely"));
						// implies the jar was registered for the job graph (otherwise the jar name would not occur in the exception)
						// implies the jar was uploaded (otherwise the file would not be found at all)
						assertThat(message, containsString("empty.jar"));
						// ensure that no stdout/stderr has been captured
						assertThat(message, containsString("System.out: " + expectedCapturedStdOut));
						assertThat(message, containsString("System.err: " + expectedCapturedStdErr));
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

	private static void createJarFile(Path zipFile) throws IOException {
		URI uri = URI.create("jar:file:" + zipFile.toString());
		HashMap<String, Object> env = new HashMap<>();
		// We need this to ensure the file will be created if it does not exist
		env.put("create", "true");
		try (FileSystem zipfs = FileSystems.newFileSystem(uri, env)) {
			Files.createDirectory(zipfs.getPath("META-INF"));
			Path manifest = zipfs.getPath("META-INF/MANIFEST.MF");
			Files.write(manifest, "Manifest-Version: 1.0\nCreated-By: Apache Flink\nMain-Class: HelloWorld\n".getBytes());

			Path content = zipfs.getPath("HelloWorld.class");
			Files.write(content, new byte[] {
				/*  // This byte array is the byte code of the following program:
				 *	public class HelloWorld {
				 *	  public static void main(String[] args) {
				 *		System.out.println("hello out!");
				 *		System.err.println("hello err!");
				 *	  }
				 *	}
				 */
				-54, -2, -70, -66, 0, 0, 0, 52, 0, 39, 10, 0, 8, 0, 22, 9, 0, 23, 0, 24, 8, 0, 25, 10, 0, 26, 0, 27, 9, 0, 23, 0, 28, 8, 0, 29, 7, 0, 30, 7, 0, 31, 1, 0, 6, 60, 105, 110, 105, 116, 62, 1, 0, 3, 40, 41, 86, 1, 0, 4, 67, 111, 100, 101, 1, 0, 15, 76, 105, 110, 101, 78, 117, 109, 98, 101, 114, 84, 97, 98, 108, 101, 1, 0, 18, 76, 111, 99, 97, 108, 86, 97, 114, 105, 97, 98, 108, 101, 84, 97, 98, 108, 101, 1, 0, 4, 116, 104, 105, 115, 1, 0, 12, 76, 72, 101, 108, 108, 111, 87, 111, 114, 108, 100, 59, 1, 0, 4, 109, 97, 105, 110, 1, 0, 22, 40, 91, 76, 106, 97, 118, 97, 47, 108, 97, 110, 103, 47, 83, 116, 114, 105, 110, 103, 59, 41, 86, 1, 0, 4, 97, 114, 103, 115, 1, 0, 19, 91, 76, 106, 97, 118, 97, 47, 108, 97, 110, 103, 47, 83, 116, 114, 105, 110, 103, 59, 1, 0, 10, 83, 111, 117, 114, 99, 101, 70, 105, 108, 101, 1, 0, 15, 72, 101, 108, 108, 111, 87, 111, 114, 108, 100, 46, 106, 97, 118, 97, 12, 0, 9, 0, 10, 7, 0, 32, 12, 0, 33, 0, 34, 1, 0, 10, 104, 101, 108, 108, 111, 32, 111, 117, 116, 33, 7, 0, 35, 12, 0, 36, 0, 37, 12, 0, 38, 0, 34, 1, 0, 10, 104, 101, 108, 108, 111, 32, 101, 114, 114, 33, 1, 0, 10, 72, 101, 108, 108, 111, 87, 111, 114, 108, 100, 1, 0, 16, 106, 97, 118, 97, 47, 108, 97, 110, 103, 47, 79, 98, 106, 101, 99, 116, 1, 0, 16, 106, 97, 118, 97, 47, 108, 97, 110, 103, 47, 83, 121, 115, 116, 101, 109, 1, 0, 3, 111, 117, 116, 1, 0, 21, 76, 106, 97, 118, 97, 47, 105, 111, 47, 80, 114, 105, 110, 116, 83, 116, 114, 101, 97, 109, 59, 1, 0, 19, 106, 97, 118, 97, 47, 105, 111, 47, 80, 114, 105, 110, 116, 83, 116, 114, 101, 97, 109, 1, 0, 7, 112, 114, 105, 110, 116, 108, 110, 1, 0, 21, 40, 76, 106, 97, 118, 97, 47, 108, 97, 110, 103, 47, 83, 116, 114, 105, 110, 103, 59, 41, 86, 1, 0, 3, 101, 114, 114, 0, 33, 0, 7, 0, 8, 0, 0, 0, 0, 0, 2, 0, 1, 0, 9, 0, 10, 0, 1, 0, 11, 0, 0, 0, 47, 0, 1, 0, 1, 0, 0, 0, 5, 42, -73, 0, 1, -79, 0, 0, 0, 2, 0, 12, 0, 0, 0, 6, 0, 1, 0, 0, 0, 19, 0, 13, 0, 0, 0, 12, 0, 1, 0, 0, 0, 5, 0, 14, 0, 15, 0, 0, 0, 9, 0, 16, 0, 17, 0, 1, 0, 11, 0, 0, 0, 67, 0, 2, 0, 1, 0, 0, 0, 17, -78, 0, 2, 18, 3, -74, 0, 4, -78, 0, 5, 18, 6, -74, 0, 4, -79, 0, 0, 0, 2, 0, 12, 0, 0, 0, 14, 0, 3, 0, 0, 0, 22, 0, 8, 0, 23, 0, 16, 0, 24, 0, 13, 0, 0, 0, 12, 0, 1, 0, 0, 0, 17, 0, 18, 0, 19, 0, 0, 0, 1, 0, 20, 0, 0, 0, 2, 0, 21
			});
		}
	}
}
