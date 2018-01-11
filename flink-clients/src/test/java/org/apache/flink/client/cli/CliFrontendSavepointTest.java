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

package org.apache.flink.client.cli;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.cli.util.MockedCliFrontend;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.zip.ZipOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the SAVEPOINT command.
 */
public class CliFrontendSavepointTest extends TestLogger {

	private static PrintStream stdOut;
	private static PrintStream stdErr;
	private static ByteArrayOutputStream buffer;

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	// ------------------------------------------------------------------------
	// Trigger savepoint
	// ------------------------------------------------------------------------

	@Test
	public void testTriggerSavepointSuccess() throws Exception {
		replaceStdOutAndStdErr();

		JobID jobId = new JobID();

		String savepointPath = "expectedSavepointPath";

		final ClusterClient clusterClient = createClusterClient(savepointPath);

		try {
			MockedCliFrontend frontend = new MockedCliFrontend(clusterClient);

			String[] parameters = { jobId.toString() };
			int returnCode = frontend.savepoint(parameters);

			assertEquals(0, returnCode);
			verify(clusterClient, times(1))
				.triggerSavepoint(eq(jobId), isNull(String.class));

			assertTrue(buffer.toString().contains(savepointPath));
		}
		finally {
			clusterClient.shutdown();
			restoreStdOutAndStdErr();
		}
	}

	@Test
	public void testTriggerSavepointFailure() throws Exception {
		replaceStdOutAndStdErr();

		JobID jobId = new JobID();

		String expectedTestException = "expectedTestException";
		Exception testException = new Exception(expectedTestException);

		final ClusterClient clusterClient = createFailingClusterClient(testException);

		try {
			MockedCliFrontend frontend = new MockedCliFrontend(clusterClient);

			String[] parameters = { jobId.toString() };

			try {
				frontend.savepoint(parameters);
				fail("This should have failed.");
			} catch (Exception e) {
				assertTrue(ExceptionUtils.findThrowableWithMessage(e, expectedTestException).isPresent());
			}
		}
		finally {
			clusterClient.shutdown();
			restoreStdOutAndStdErr();
		}
	}

	@Test
	public void testTriggerSavepointFailureIllegalJobID() throws Exception {
		replaceStdOutAndStdErr();

		try {
			CliFrontend frontend = new MockedCliFrontend(new StandaloneClusterClient(
				new Configuration(),
				new TestingHighAvailabilityServices()));

			String[] parameters = { "invalid job id" };

			try {
				frontend.savepoint(parameters);
				fail("Should have failed.");
			} catch (CliArgsException e) {
				assertThat(e.getMessage(), Matchers.containsString("Cannot parse JobID"));
			}
		}
		finally {
			restoreStdOutAndStdErr();
		}
	}

	/**
	 * Tests that a CLI call with a custom savepoint directory target is
	 * forwarded correctly to the cluster client.
	 */
	@Test
	public void testTriggerSavepointCustomTarget() throws Exception {
		replaceStdOutAndStdErr();

		JobID jobId = new JobID();

		String savepointDirectory = "customTargetDirectory";

		final ClusterClient clusterClient = createClusterClient(savepointDirectory);

		try {
			MockedCliFrontend frontend = new MockedCliFrontend(clusterClient);

			String[] parameters = { jobId.toString(), savepointDirectory };
			int returnCode = frontend.savepoint(parameters);

			assertEquals(0, returnCode);
			verify(clusterClient, times(1))
				.triggerSavepoint(eq(jobId), eq(savepointDirectory));

			assertTrue(buffer.toString().contains(savepointDirectory));
		}
		finally {
			clusterClient.shutdown();

			restoreStdOutAndStdErr();
		}
	}

	// ------------------------------------------------------------------------
	// Dispose savepoint
	// ------------------------------------------------------------------------

	@Test
	public void testDisposeSavepointSuccess() throws Exception {
		replaceStdOutAndStdErr();

		String savepointPath = "expectedSavepointPath";

		ClusterClient clusterClient = new DisposeSavepointClusterClient(
			(String path, Time timeout) -> CompletableFuture.completedFuture(Acknowledge.get()));

		try {

			CliFrontend frontend = new MockedCliFrontend(clusterClient);

			String[] parameters = { "-d", savepointPath };
			frontend.savepoint(parameters);

			String outMsg = buffer.toString();
			assertTrue(outMsg.contains(savepointPath));
			assertTrue(outMsg.contains("disposed"));
		}
		finally {
			clusterClient.shutdown();
			restoreStdOutAndStdErr();
		}
	}

	/**
	 * Tests disposal with a JAR file.
	 */
	@Test
	public void testDisposeWithJar() throws Exception {
		replaceStdOutAndStdErr();

		final CompletableFuture<String> disposeSavepointFuture = new CompletableFuture<>();

		final ClusterClient clusterClient = new DisposeSavepointClusterClient(
			(String savepointPath, Time timeout) -> {
				disposeSavepointFuture.complete(savepointPath);
				return CompletableFuture.completedFuture(Acknowledge.get());
			});

		try {
			CliFrontend frontend = new MockedCliFrontend(clusterClient);

			// Fake JAR file
			File f = tmp.newFile();
			ZipOutputStream out = new ZipOutputStream(new FileOutputStream(f));
			out.close();

			final String disposePath = "any-path";
			String[] parameters = { "-d", disposePath, "-j", f.getAbsolutePath() };

			frontend.savepoint(parameters);

			final String actualSavepointPath = disposeSavepointFuture.get();

			assertEquals(disposePath, actualSavepointPath);
		} finally {
			clusterClient.shutdown();
			restoreStdOutAndStdErr();
		}
	}

	@Test
	public void testDisposeSavepointFailure() throws Exception {
		replaceStdOutAndStdErr();

		String savepointPath = "expectedSavepointPath";

		Exception testException = new Exception("expectedTestException");

		ClusterClient clusterClient = new DisposeSavepointClusterClient((String path, Time timeout) -> FutureUtils.completedExceptionally(testException));

		try {
			CliFrontend frontend = new MockedCliFrontend(clusterClient);

			String[] parameters = { "-d", savepointPath };

			int returnCode = frontend.savepoint(parameters);

			assertNotEquals(0, returnCode);

			assertTrue(buffer.toString().contains(testException.getMessage()));
		}
		finally {
			clusterClient.shutdown();
			restoreStdOutAndStdErr();
		}
	}

	// ------------------------------------------------------------------------

	private static final class DisposeSavepointClusterClient extends StandaloneClusterClient {

		final BiFunction<String, Time, CompletableFuture<Acknowledge>> disposeSavepointFunction;

		public DisposeSavepointClusterClient(BiFunction<String, Time, CompletableFuture<Acknowledge>> disposeSavepointFunction) throws Exception {
			super(new Configuration(), new TestingHighAvailabilityServices());

			this.disposeSavepointFunction = Preconditions.checkNotNull(disposeSavepointFunction);
		}

		@Override
		public CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath, Time timeout) {
			return disposeSavepointFunction.apply(savepointPath, timeout);
		}
	}

	private static void replaceStdOutAndStdErr() {
		stdOut = System.out;
		stdErr = System.err;
		buffer = new ByteArrayOutputStream();
		PrintStream capture = new PrintStream(buffer);
		System.setOut(capture);
		System.setErr(capture);
	}

	private static void restoreStdOutAndStdErr() {
		System.setOut(stdOut);
		System.setErr(stdErr);
	}

	private static ClusterClient createClusterClient(String expectedResponse) throws Exception {
		final ClusterClient clusterClient = mock(ClusterClient.class);

		when(clusterClient.triggerSavepoint(any(JobID.class), anyString()))
			.thenReturn(CompletableFuture.completedFuture(expectedResponse));

		return clusterClient;
	}

	private static ClusterClient createFailingClusterClient(Exception expectedException) throws Exception {
		final ClusterClient clusterClient = mock(ClusterClient.class);

		when(clusterClient.triggerSavepoint(any(JobID.class), anyString()))
			.thenReturn(FutureUtils.completedExceptionally(expectedException));

		return clusterClient;
	}
}
