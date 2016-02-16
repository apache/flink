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

package org.apache.flink.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.CommandLineOptions;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.junit.Test;
import org.mockito.Mockito;
import scala.concurrent.Promise;
import scala.concurrent.duration.FiniteDuration;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CliFrontendSavepointTest {

	private static PrintStream stdOut;
	private static PrintStream stdErr;
	private static ByteArrayOutputStream buffer;

	// ------------------------------------------------------------------------
	// Trigger savepoint
	// ------------------------------------------------------------------------

	@Test
	public void testTriggerSavepointSuccess() throws Exception {
		replaceStdOutAndStdErr();

		try {
			JobID jobId = new JobID();
			ActorGateway jobManager = mock(ActorGateway.class);

			Promise<Object> triggerResponse = new scala.concurrent.impl.Promise.DefaultPromise<>();

			when(jobManager.ask(
					Mockito.eq(new JobManagerMessages.TriggerSavepoint(jobId)),
					Mockito.any(FiniteDuration.class)))
					.thenReturn(triggerResponse.future());

			String savepointPath = "expectedSavepointPath";

			triggerResponse.success(new JobManagerMessages
					.TriggerSavepointSuccess(jobId, savepointPath));

			CliFrontend frontend = new MockCliFrontend(
					CliFrontendTestUtils.getConfigDir(), jobManager);

			String[] parameters = { jobId.toString() };
			int returnCode = frontend.savepoint(parameters);

			assertEquals(0, returnCode);
			verify(jobManager, times(1)).ask(
					Mockito.eq(new JobManagerMessages.TriggerSavepoint(jobId)),
					Mockito.any(FiniteDuration.class));

			assertTrue(buffer.toString().contains("expectedSavepointPath"));
		}
		finally {
			restoreStdOutAndStdErr();
		}
	}

	@Test
	public void testTriggerSavepointFailure() throws Exception {
		replaceStdOutAndStdErr();

		try {
			JobID jobId = new JobID();
			ActorGateway jobManager = mock(ActorGateway.class);

			Promise<Object> triggerResponse = new scala.concurrent.impl.Promise.DefaultPromise<>();

			when(jobManager.ask(
					Mockito.eq(new JobManagerMessages.TriggerSavepoint(jobId)),
					Mockito.any(FiniteDuration.class)))
					.thenReturn(triggerResponse.future());

			Exception testException = new Exception("expectedTestException");

			triggerResponse.success(new JobManagerMessages
					.TriggerSavepointFailure(jobId, testException));

			CliFrontend frontend = new MockCliFrontend(
					CliFrontendTestUtils.getConfigDir(), jobManager);

			String[] parameters = { jobId.toString() };
			int returnCode = frontend.savepoint(parameters);

			assertTrue(returnCode != 0);
			verify(jobManager, times(1)).ask(
					Mockito.eq(new JobManagerMessages.TriggerSavepoint(jobId)),
					Mockito.any(FiniteDuration.class));

			assertTrue(buffer.toString().contains("expectedTestException"));
		}
		finally {
			restoreStdOutAndStdErr();
		}
	}

	@Test
	public void testTriggerSavepointFailureIllegalJobID() throws Exception {
		replaceStdOutAndStdErr();

		try {
			CliFrontend frontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());

			String[] parameters = { "invalid job id" };
			int returnCode = frontend.savepoint(parameters);

			assertTrue(returnCode != 0);
			assertTrue(buffer.toString().contains("not a valid ID"));
		}
		finally {
			restoreStdOutAndStdErr();
		}
	}

	@Test
	public void testTriggerSavepointFailureUnknownResponse() throws Exception {
		replaceStdOutAndStdErr();

		try {
			JobID jobId = new JobID();
			ActorGateway jobManager = mock(ActorGateway.class);

			Promise<Object> triggerResponse = new scala.concurrent.impl.Promise.DefaultPromise<>();

			when(jobManager.ask(
					Mockito.eq(new JobManagerMessages.TriggerSavepoint(jobId)),
					Mockito.any(FiniteDuration.class)))
					.thenReturn(triggerResponse.future());

			triggerResponse.success("UNKNOWN RESPONSE");

			CliFrontend frontend = new MockCliFrontend(
					CliFrontendTestUtils.getConfigDir(), jobManager);

			String[] parameters = { jobId.toString() };
			int returnCode = frontend.savepoint(parameters);

			assertTrue(returnCode != 0);
			verify(jobManager, times(1)).ask(
					Mockito.eq(new JobManagerMessages.TriggerSavepoint(jobId)),
					Mockito.any(FiniteDuration.class));

			String errMsg = buffer.toString();
			assertTrue(errMsg.contains("IllegalStateException"));
			assertTrue(errMsg.contains("Unknown JobManager response"));
		}
		finally {
			restoreStdOutAndStdErr();
		}
	}

	// ------------------------------------------------------------------------
	// Dispose savepoint
	// ------------------------------------------------------------------------

	@Test
	public void testDisposeSavepointSuccess() throws Exception {
		replaceStdOutAndStdErr();

		try {
			String savepointPath = "expectedSavepointPath";
			ActorGateway jobManager = mock(ActorGateway.class);

			Promise<Object> triggerResponse = new scala.concurrent.impl.Promise.DefaultPromise<>();

			when(jobManager.ask(
					Mockito.eq(new JobManagerMessages.DisposeSavepoint(savepointPath)),
					Mockito.any(FiniteDuration.class))).thenReturn(triggerResponse.future());

			triggerResponse.success(JobManagerMessages.getDisposeSavepointSuccess());

			CliFrontend frontend = new MockCliFrontend(
					CliFrontendTestUtils.getConfigDir(), jobManager);

			String[] parameters = { "-d", savepointPath };
			int returnCode = frontend.savepoint(parameters);

			assertEquals(0, returnCode);
			verify(jobManager, times(1)).ask(
					Mockito.eq(new JobManagerMessages.DisposeSavepoint(savepointPath)),
					Mockito.any(FiniteDuration.class));

			String outMsg = buffer.toString();
			assertTrue(outMsg.contains(savepointPath));
			assertTrue(outMsg.contains("disposed"));
		}
		finally {
			restoreStdOutAndStdErr();
		}
	}

	@Test
	public void testDisposeSavepointFailure() throws Exception {
		replaceStdOutAndStdErr();

		try {
			String savepointPath = "expectedSavepointPath";
			ActorGateway jobManager = mock(ActorGateway.class);

			Promise<Object> triggerResponse = new scala.concurrent.impl.Promise.DefaultPromise<>();

			when(jobManager.ask(
					Mockito.eq(new JobManagerMessages.DisposeSavepoint(savepointPath)),
					Mockito.any(FiniteDuration.class)))
					.thenReturn(triggerResponse.future());

			Exception testException = new Exception("expectedTestException");

			triggerResponse.success(new JobManagerMessages
					.DisposeSavepointFailure(testException));

			CliFrontend frontend = new MockCliFrontend(
					CliFrontendTestUtils.getConfigDir(), jobManager);

			String[] parameters = { "-d", savepointPath };
			int returnCode = frontend.savepoint(parameters);

			assertTrue(returnCode != 0);
			verify(jobManager, times(1)).ask(
					Mockito.eq(new JobManagerMessages.DisposeSavepoint(savepointPath)),
					Mockito.any(FiniteDuration.class));

			assertTrue(buffer.toString().contains("expectedTestException"));
		}
		finally {
			restoreStdOutAndStdErr();
		}
	}

	@Test
	public void testDisposeSavepointFailureUnknownResponse() throws Exception {
		replaceStdOutAndStdErr();

		try {
			String savepointPath = "expectedSavepointPath";
			ActorGateway jobManager = mock(ActorGateway.class);

			Promise<Object> triggerResponse = new scala.concurrent.impl.Promise.DefaultPromise<>();

			when(jobManager.ask(
					Mockito.eq(new JobManagerMessages.DisposeSavepoint(savepointPath)),
					Mockito.any(FiniteDuration.class)))
					.thenReturn(triggerResponse.future());

			triggerResponse.success("UNKNOWN RESPONSE");

			CliFrontend frontend = new MockCliFrontend(
					CliFrontendTestUtils.getConfigDir(), jobManager);

			String[] parameters = { "-d", savepointPath };
			int returnCode = frontend.savepoint(parameters);

			assertTrue(returnCode != 0);
			verify(jobManager, times(1)).ask(
					Mockito.eq(new JobManagerMessages.DisposeSavepoint(savepointPath)),
					Mockito.any(FiniteDuration.class));

			String errMsg = buffer.toString();
			assertTrue(errMsg.contains("IllegalStateException"));
			assertTrue(errMsg.contains("Unknown JobManager response"));
		}
		finally {
			restoreStdOutAndStdErr();
		}

		replaceStdOutAndStdErr();
	}

	// ------------------------------------------------------------------------

	private static class MockCliFrontend extends CliFrontend {

		private final ActorGateway mockJobManager;

		public MockCliFrontend(String configDir, ActorGateway mockJobManager) throws Exception {
			super(configDir);
			this.mockJobManager = mockJobManager;
		}

		@Override
		protected ActorGateway getJobManagerGateway(CommandLineOptions options) throws Exception {
			return mockJobManager;
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
}
