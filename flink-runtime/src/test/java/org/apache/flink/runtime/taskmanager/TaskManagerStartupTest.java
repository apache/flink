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

package org.apache.flink.runtime.taskmanager;

import static org.junit.Assert.*;

import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.UUID;

/**
 * Tests that check how the TaskManager behaves when encountering startup problems.
 */
public class TaskManagerStartupTest {

	/**
	 * Tests that the TaskManager fails synchronously when the actor system port
	 * is in use.
	 */
	@Test
	public void testStartupWhenTaskmanagerActorPortIsUsed() {
		ServerSocket blocker = null;
		try {
			final String localHostName = "localhost";
			final InetAddress localAddress = InetAddress.getByName(localHostName);

			// block some port
			blocker = new ServerSocket(0, 50, localAddress);
			final int port = blocker.getLocalPort();

			try {
				TaskManager.runTaskManager(
					localHostName,
					port,
					new Configuration(),
					TaskManager.class);
				fail("This should fail with an IOException");
			}
			catch (IOException e) {
				// expected. validate the error message
				assertNotNull(e.getMessage());
				assertTrue(e.getMessage().contains("Address already in use"));
			}

		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (blocker != null) {
				try {
					blocker.close();
				}
				catch (IOException e) {
					// no need to log here
				}
			}
		}
	}

	/**
	 * Tests that the TaskManager startup fails synchronously when the I/O directories are
	 * not writable.
	 */
	@Test
	public void testIODirectoryNotWritable() {
		File tempDir = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH);
		File nonWritable = new File(tempDir, UUID.randomUUID().toString());

		if (!nonWritable.mkdirs() || !nonWritable.setWritable(false, false)) {
			System.err.println("Cannot create non-writable temporary file directory. Skipping test.");
			return;
		}

		try {
			Configuration cfg = new Configuration();
			cfg.setString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, nonWritable.getAbsolutePath());
			cfg.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 4);
			cfg.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
			cfg.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 21656);

			try {
				TaskManager.runTaskManager("localhost", 0, cfg);
				fail("Should fail synchronously with an exception");
			}
			catch (IOException e) {
				// splendid!
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			//noinspection ResultOfMethodCallIgnored
			nonWritable.setWritable(true, false);
			try {
				FileUtils.deleteDirectory(nonWritable);
			}
			catch (IOException e) {
				// best effort
			}
		}
	}

	/**
	 * Tests that the TaskManager startup fails synchronously when the I/O directories are
	 * not writable.
	 */
	@Test
	public void testMemoryConfigWrong() {
		try {
			Configuration cfg = new Configuration();
			cfg.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
			cfg.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 21656);
			cfg.setString(ConfigConstants.TASK_MANAGER_MEMORY_PRE_ALLOCATE_KEY, "true");

			// something invalid
			cfg.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, -42);
			try {
				TaskManager.runTaskManager("localhost", 0, cfg);
				fail("Should fail synchronously with an exception");
			}
			catch (IllegalConfigurationException e) {
				// splendid!
			}

			// something ridiculously high
			final long memSize = (((long) Integer.MAX_VALUE - 1) *
									ConfigConstants.DEFAULT_TASK_MANAGER_MEMORY_SEGMENT_SIZE) >> 20;
			cfg.setLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, memSize);
			try {
				TaskManager.runTaskManager("localhost", 0, cfg);
				fail("Should fail synchronously with an exception");
			}
			catch (Exception e) {
				// splendid!
				assertTrue(e.getCause() instanceof OutOfMemoryError);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
