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
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link JarDeleteHandler}.
 */
public class JarDeleteHandlerTest extends TestLogger {

	private static final String TEST_JAR_NAME = "test.jar";

	private JarDeleteHandler jarDeleteHandler;

	private RestfulGateway restfulGateway;

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private Path jarDir;

	@Before
	public void setUp() throws Exception {
		jarDir = temporaryFolder.newFolder().toPath();
		restfulGateway = TestingRestfulGateway.newBuilder().build();
		jarDeleteHandler = new JarDeleteHandler(
			() -> CompletableFuture.completedFuture(restfulGateway),
			Time.seconds(10),
			Collections.emptyMap(),
			new JarDeleteHeaders(),
			jarDir,
			Executors.directExecutor()
		);

		Files.createFile(jarDir.resolve(TEST_JAR_NAME));
	}

	@Test
	public void testDeleteJarById() throws Exception {
		assertThat(Files.exists(jarDir.resolve(TEST_JAR_NAME)), equalTo(true));

		final HandlerRequest<EmptyRequestBody, JarDeleteMessageParameters> request = createRequest(TEST_JAR_NAME);
		jarDeleteHandler.handleRequest(request, restfulGateway).get();

		assertThat(Files.exists(jarDir.resolve(TEST_JAR_NAME)), equalTo(false));
	}

	@Test
	public void testDeleteUnknownJar() throws Exception {
		final HandlerRequest<EmptyRequestBody, JarDeleteMessageParameters> request = createRequest("doesnotexist.jar");
		try {
			jarDeleteHandler.handleRequest(request, restfulGateway).get();
		} catch (final ExecutionException e) {
			final Throwable throwable = ExceptionUtils.stripCompletionException(e.getCause());
			assertThat(throwable, instanceOf(RestHandlerException.class));

			final RestHandlerException restHandlerException = (RestHandlerException) throwable;
			assertThat(restHandlerException.getMessage(), containsString("File doesnotexist.jar does not exist in"));
			assertThat(restHandlerException.getHttpResponseStatus(), equalTo(HttpResponseStatus.BAD_REQUEST));
		}
	}

	@Test
	public void testFailedDelete() throws Exception {
		makeJarDirReadOnly();

		final HandlerRequest<EmptyRequestBody, JarDeleteMessageParameters> request = createRequest(TEST_JAR_NAME);
		try {
			jarDeleteHandler.handleRequest(request, restfulGateway).get();
		} catch (final ExecutionException e) {
			final Throwable throwable = ExceptionUtils.stripCompletionException(e.getCause());
			assertThat(throwable, instanceOf(RestHandlerException.class));

			final RestHandlerException restHandlerException = (RestHandlerException) throwable;
			assertThat(restHandlerException.getMessage(), containsString("Failed to delete jar"));
			assertThat(restHandlerException.getHttpResponseStatus(), equalTo(HttpResponseStatus.INTERNAL_SERVER_ERROR));
		}
	}

	private static HandlerRequest<EmptyRequestBody, JarDeleteMessageParameters> createRequest(
			final String jarFileName) throws HandlerRequestException {
		return new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new JarDeleteMessageParameters(),
			Collections.singletonMap(JarIdPathParameter.KEY, jarFileName),
			Collections.emptyMap());
	}

	private void makeJarDirReadOnly() {
		try {
			Files.setPosixFilePermissions(jarDir, new HashSet<>(Arrays.asList(
				PosixFilePermission.OTHERS_READ,
				PosixFilePermission.GROUP_READ,
				PosixFilePermission.OWNER_READ,
				PosixFilePermission.OTHERS_EXECUTE,
				PosixFilePermission.GROUP_EXECUTE,
				PosixFilePermission.OWNER_EXECUTE)));
		} catch (final Exception e) {
			Assume.assumeNoException(e);
		}
	}

}
