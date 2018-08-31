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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServices;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.StartupUtils;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorSystem;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

import scala.Option;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that check how the TaskManager behaves when encountering startup
 * problems.
 */
public class TaskManagerStartupTest extends TestLogger {
	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	private HighAvailabilityServices highAvailabilityServices;

	@Before
	public void setupTest() {
		highAvailabilityServices = new EmbeddedHaServices(TestingUtils.defaultExecutor());
	}

	@After
	public void tearDownTest() throws Exception {
		if (highAvailabilityServices != null) {
			highAvailabilityServices.closeAndCleanupAllData();
			highAvailabilityServices = null;
		}
	}
	

	/**
	 * Tests that the TaskManager fails synchronously when the actor system port
	 * is in use.
	 * 
	 * @throws Throwable
	 */
	@Test(expected = BindException.class)
	public void testStartupWhenTaskmanagerActorPortIsUsed() throws Exception {
		ServerSocket blocker = null;

		try {
			final String localHostName = "localhost";
			final InetAddress localBindAddress = InetAddress.getByName(NetUtils.getWildcardIPAddress());

			// block some port
			blocker = new ServerSocket(0, 50, localBindAddress);
			final int port = blocker.getLocalPort();

			TaskManager.runTaskManager(
				localHostName,
				ResourceID.generate(),
				port,
				new Configuration(),
				highAvailabilityServices,
				TaskManager.class);
			fail("This should fail with an IOException");

		}
		catch (Exception e) {
			// expected. validate the error message
			List<Throwable> causes = StartupUtils.getExceptionCauses(e, new ArrayList<Throwable>());
			for (Throwable cause : causes) {
				if (cause instanceof BindException) {
					throw (BindException) cause;
				}
			}

			fail("This should fail with an exception caused by BindException");
		}
		finally {
			if (blocker != null) {
				try {
					blocker.close();
				} catch (IOException e) {
					// no need to log here
				}
			}

			highAvailabilityServices.closeAndCleanupAllData();
		}
	}



	/**
	 * Tests that the TaskManager startup fails synchronously when the I/O
	 * directories are not writable.
	 */
	@Test
	public void testIODirectoryNotWritable() throws Exception {
		File nonWritable = tempFolder.newFolder();
		Assume.assumeTrue("Cannot create non-writable temporary file directory. Skipping test.",
			nonWritable.setWritable(false, false));

		try {
			Configuration cfg = new Configuration();
			cfg.setString(CoreOptions.TMP_DIRS, nonWritable.getAbsolutePath());
			cfg.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "4m");
			cfg.setString(JobManagerOptions.ADDRESS, "localhost");
			cfg.setInteger(JobManagerOptions.PORT, 21656);

			try {
				TaskManager.runTaskManager(
					"localhost",
					ResourceID.generate(),
					0,
					cfg,
					highAvailabilityServices);
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
			// noinspection ResultOfMethodCallIgnored
			nonWritable.setWritable(true, false);
			try {
				FileUtils.deleteDirectory(nonWritable);
			}
			catch (IOException e) {
				// best effort
			}

			highAvailabilityServices.closeAndCleanupAllData();
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
			cfg.setString(JobManagerOptions.ADDRESS, "localhost");
			cfg.setInteger(JobManagerOptions.PORT, 21656);
			cfg.setBoolean(TaskManagerOptions.MANAGED_MEMORY_PRE_ALLOCATE, true);

			// something invalid
			cfg.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "-42m");
			try {
				TaskManager.runTaskManager(
					"localhost",
					ResourceID.generate(),
					0,
					cfg,
					highAvailabilityServices);
				fail("Should fail synchronously with an exception");
			}
			catch (IllegalConfigurationException e) {
				// splendid!
			}

			// something ridiculously high
			final long memSize = (((long) Integer.MAX_VALUE - 1) *
				MemorySize.parse(TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue()).getBytes()) >> 20;
			cfg.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, memSize + "m");
			try {
				TaskManager.runTaskManager(
					"localhost",
					ResourceID.generate(),
					0,
					cfg,
					highAvailabilityServices);
				fail("Should fail synchronously with an exception");
			}
			catch (Exception e) {
				// splendid!
				assertTrue(e.getCause() instanceof OutOfMemoryError);
			}
		} 
		catch(Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Tests that the task manager start-up fails if the network stack cannot be initialized.
	 * @throws Exception
	 */
	@Test(expected = IOException.class)
	public void testStartupWhenNetworkStackFailsToInitialize() throws Exception {

		ServerSocket blocker = null;

		try {
			blocker = new ServerSocket(0, 50, InetAddress.getByName("localhost"));

			final Configuration cfg = new Configuration();
			cfg.setString(ConfigConstants.TASK_MANAGER_HOSTNAME_KEY, "localhost");
			cfg.setInteger(TaskManagerOptions.DATA_PORT, blocker.getLocalPort());
			cfg.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "1m");
			ActorSystem actorSystem = AkkaUtils.createLocalActorSystem(cfg);
			TaskManager.startTaskManagerComponentsAndActor(
				cfg,
				ResourceID.generate(),
				actorSystem,
				highAvailabilityServices,
				NoOpMetricRegistry.INSTANCE,
				"localhost",
				Option.<String>empty(),
				false,
				TaskManager.class);
		}
		finally {
			if (blocker != null) {
				try {
					blocker.close();
				}
				catch (IOException e) {
					// ignore, best effort
				}
			}
		}
	}
}
