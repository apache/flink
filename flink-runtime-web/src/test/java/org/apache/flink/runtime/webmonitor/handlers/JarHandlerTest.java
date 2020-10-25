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

import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for the {@link JarRunHandler} and  {@link JarPlanHandler}.
 */
public class JarHandlerTest extends TestLogger {

	private static final String JAR_NAME = "output-test-program.jar";

	@ClassRule
	public static final TemporaryFolder TMP = new TemporaryFolder();

	@Test
	public void testPlanJar() throws Exception {
		runTest("hello out!", "hello err!");
	}

	private static void runTest(String expectedCapturedStdOut, String expectedCapturedStdErr) throws Exception {
		final TestingDispatcherGateway restfulGateway = new TestingDispatcherGateway.Builder().build();

		final JarHandlers handlers = new JarHandlers(TMP.newFolder().toPath(), restfulGateway);

		final Path originalJar = Paths.get(System.getProperty("targetDir")).resolve(JAR_NAME);
		final Path jar = Files.copy(originalJar, TMP.newFolder().toPath().resolve(JAR_NAME));

		final String storedJarPath = JarHandlers.uploadJar(handlers.uploadHandler, jar, restfulGateway);
		final String storedJarName = Paths.get(storedJarPath).getFileName().toString();

		try {
			JarHandlers.showPlan(handlers.planHandler, storedJarName, restfulGateway);
			Assert.fail("Should have failed with an exception.");
		} catch (Exception e) {
			Optional<ProgramInvocationException> expected = ExceptionUtils.findThrowable(e, ProgramInvocationException.class);
			if (expected.isPresent()) {
				String message = expected.get().getMessage();
				// original cause is preserved in stack trace
				assertThat(message, containsString("The program plan could not be fetched - the program aborted pre-maturely"));
				// implies the jar was registered for the job graph (otherwise the jar name would not occur in the exception)
				assertThat(message, containsString(JAR_NAME));
				// ensure that no stdout/stderr has been captured
				assertThat(message, containsString("System.out: " + expectedCapturedStdOut));
				assertThat(message, containsString("System.err: " + expectedCapturedStdErr));
			} else {
				throw e;
			}
		}
	}
}
