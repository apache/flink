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

package org.apache.flink.runtime.checkpoint.hooks;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for the MasterHooks utility class.
 */
public class MasterHooksTest extends TestLogger {

	// ------------------------------------------------------------------------
	//  hook management
	// ------------------------------------------------------------------------

	@Test
	public void wrapHook() throws Exception {
		final String id = "id";

		Thread thread = Thread.currentThread();
		final ClassLoader originalClassLoader = thread.getContextClassLoader();
		final ClassLoader userClassLoader = new URLClassLoader(new URL[0]);

		final Runnable command = spy(new Runnable() {
			@Override
			public void run() {
				assertEquals(userClassLoader, Thread.currentThread().getContextClassLoader());
			}
		});

		MasterTriggerRestoreHook<String> hook = spy(new MasterTriggerRestoreHook<String>() {
			@Override
			public String getIdentifier() {
				assertEquals(userClassLoader, Thread.currentThread().getContextClassLoader());
				return id;
			}

			@Override
			public void reset() throws Exception {
				assertEquals(userClassLoader, Thread.currentThread().getContextClassLoader());
			}

			@Override
			public void close() throws Exception {
				assertEquals(userClassLoader, Thread.currentThread().getContextClassLoader());
			}

			@Nullable
			@Override
			public CompletableFuture<String> triggerCheckpoint(long checkpointId, long timestamp, Executor executor) throws Exception {
				assertEquals(userClassLoader, Thread.currentThread().getContextClassLoader());
				executor.execute(command);
				return null;
			}

			@Override
			public void restoreCheckpoint(long checkpointId, @Nullable String checkpointData) throws Exception {
				assertEquals(userClassLoader, Thread.currentThread().getContextClassLoader());
			}

			@Nullable
			@Override
			public SimpleVersionedSerializer<String> createCheckpointDataSerializer() {
				assertEquals(userClassLoader, Thread.currentThread().getContextClassLoader());
				return null;
			}
		});

		MasterTriggerRestoreHook<String> wrapped = MasterHooks.wrapHook(hook, userClassLoader);

		// verify getIdentifier
		wrapped.getIdentifier();
		verify(hook, times(1)).getIdentifier();
		assertEquals(originalClassLoader, thread.getContextClassLoader());

		// verify triggerCheckpoint and its wrapped executor
		TestExecutor testExecutor = new TestExecutor();
		wrapped.triggerCheckpoint(0L, 0, testExecutor);
		assertEquals(originalClassLoader, thread.getContextClassLoader());
		assertNotNull(testExecutor.command);
		testExecutor.command.run();
		verify(command, times(1)).run();
		assertEquals(originalClassLoader, thread.getContextClassLoader());

		// verify restoreCheckpoint
		wrapped.restoreCheckpoint(0L, "");
		verify(hook, times(1)).restoreCheckpoint(eq(0L), eq(""));
		assertEquals(originalClassLoader, thread.getContextClassLoader());

		// verify createCheckpointDataSerializer
		wrapped.createCheckpointDataSerializer();
		verify(hook, times(1)).createCheckpointDataSerializer();
		assertEquals(originalClassLoader, thread.getContextClassLoader());

		// verify close
		wrapped.close();
		verify(hook, times(1)).close();
		assertEquals(originalClassLoader, thread.getContextClassLoader());
	}

	private static class TestExecutor implements Executor {
		Runnable command;

		@Override
		public void execute(Runnable command) {
			this.command = command;
		}
	}
}
