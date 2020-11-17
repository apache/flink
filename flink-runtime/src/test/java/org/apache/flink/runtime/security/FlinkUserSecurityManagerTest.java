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

package org.apache.flink.runtime.security;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.UserSystemExitException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@code FlinkUserSecurityManager}.
 */
public class FlinkUserSecurityManagerTest {

	private static final int TEST_EXIT_CODE = 123;
	private FlinkUserSecurityManager flinkUserSecurityManager;

	@Before
	public void setUp() {
		flinkUserSecurityManager = new FlinkUserSecurityManager(FlinkUserSecurityManager.CheckExitMode.THROW);
		System.setSecurityManager(flinkUserSecurityManager);
	}

	@After
	public void tearDown() {
		if (flinkUserSecurityManager != null) {
			System.setSecurityManager(flinkUserSecurityManager.getOriginalSecurityManager());
		}
	}

	@Test(expected = UserSystemExitException.class)
	public void testThrowUserExit() {
		flinkUserSecurityManager.monitorSystemExit();
		flinkUserSecurityManager.checkExit(TEST_EXIT_CODE);
	}

	@Test
	public void testToggleUserExit() {
		flinkUserSecurityManager.checkExit(TEST_EXIT_CODE);
		flinkUserSecurityManager.monitorSystemExit();
		try {
			flinkUserSecurityManager.checkExit(TEST_EXIT_CODE);
			fail();
		} catch (UserSystemExitException ignored) { }
		flinkUserSecurityManager.unmonitorSystemExit();
		flinkUserSecurityManager.checkExit(TEST_EXIT_CODE);
	}

	@Test
	public void testPerThreadThrowUserExit() throws Exception {
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		// Async thread test before enabling monitoring ensures it does not throw while prestarting
		// worker thread, which is to be unmonitored and tested after enabling monitoring enabled.
		CompletableFuture<Void> future =
			CompletableFuture.runAsync(() -> flinkUserSecurityManager.checkExit(TEST_EXIT_CODE),
				executorService);
		future.get();
		flinkUserSecurityManager.monitorSystemExit();
		try {
			flinkUserSecurityManager.checkExit(TEST_EXIT_CODE);
			fail();
		} catch (UserSystemExitException ignored) { }
		// This threaded exit should be allowed as thread is not spawned while monitor is enabled.
		future = CompletableFuture.runAsync(() -> flinkUserSecurityManager.checkExit(TEST_EXIT_CODE),
				executorService);
		future.get();
	}

	@Test
	public void testInheritedThrowUserExit() throws Exception {
		flinkUserSecurityManager.monitorSystemExit();
		try {
			flinkUserSecurityManager.checkExit(TEST_EXIT_CODE);
			fail();
		} catch (UserSystemExitException ignored) { }
		Thread thread = new Thread(() -> {
			try {
				flinkUserSecurityManager.checkExit(TEST_EXIT_CODE);
				fail();
			} catch (UserSystemExitException ignored) {
			} catch (Throwable t) {
				fail();
			}
		});
		thread.start();
		thread.join();
	}

	@Test
	public void testWarnUserExit() {
		// Warn mode enables monitor but only logging allowing exit, hence not expecting exception.
		// NOTE - Do not specifically test warning logging.
		System.setSecurityManager(flinkUserSecurityManager.getOriginalSecurityManager());
		flinkUserSecurityManager = new FlinkUserSecurityManager(FlinkUserSecurityManager.CheckExitMode.WARN);
		System.setSecurityManager(flinkUserSecurityManager);
		flinkUserSecurityManager.monitorSystemExit();
		flinkUserSecurityManager.checkExit(TEST_EXIT_CODE);
	}

	@Test
	public void testValidConfiguration() {
		Configuration configuration = new Configuration();

		System.setSecurityManager(flinkUserSecurityManager.getOriginalSecurityManager());

		// Default case (no provided option) - allowing everything, so null security manager is expected.
		flinkUserSecurityManager = FlinkUserSecurityManager.fromConfiguration(configuration);
		assertNull(flinkUserSecurityManager);

		// Disabled case (same as default)
		configuration.setString(SecurityOptions.CHECK_SYSTEM_EXIT, "disabled");
		flinkUserSecurityManager = FlinkUserSecurityManager.fromConfiguration(configuration);
		assertNull(flinkUserSecurityManager);

		// Enabled - warn case (logging as warning but allowing exit)
		configuration.setString(SecurityOptions.CHECK_SYSTEM_EXIT, "warn");
		flinkUserSecurityManager = FlinkUserSecurityManager.fromConfiguration(configuration);
		assertNotNull(flinkUserSecurityManager);
		System.setSecurityManager(flinkUserSecurityManager);
		assertFalse(flinkUserSecurityManager.systemExitMonitored());
		FlinkUserSecurityManager.monitorSystemExitForCurrentThread();
		assertTrue(flinkUserSecurityManager.systemExitMonitored());
		flinkUserSecurityManager.checkExit(TEST_EXIT_CODE);
		FlinkUserSecurityManager.unmonitorSystemExitForCurrentThread();
		assertFalse(flinkUserSecurityManager.systemExitMonitored());

		// Enabled - throw case (disallowing by throwing exception)
		configuration.setString(SecurityOptions.CHECK_SYSTEM_EXIT, "throw");
		flinkUserSecurityManager = FlinkUserSecurityManager.fromConfiguration(configuration);
		assertNotNull(flinkUserSecurityManager);
		System.setSecurityManager(flinkUserSecurityManager);
		assertFalse(flinkUserSecurityManager.systemExitMonitored());
		FlinkUserSecurityManager.monitorSystemExitForCurrentThread();
		assertTrue(flinkUserSecurityManager.systemExitMonitored());
		try {
			flinkUserSecurityManager.checkExit(TEST_EXIT_CODE);
			fail();
		} catch (UserSystemExitException ignored) { }
		FlinkUserSecurityManager.unmonitorSystemExitForCurrentThread();
		assertFalse(flinkUserSecurityManager.systemExitMonitored());

		// Test for disabled test to check if exit is still allowed (fromConfiguration gives null since currently
		// there is only one option to have a valid security manager, so test with constructor).
		flinkUserSecurityManager = new FlinkUserSecurityManager(FlinkUserSecurityManager.CheckExitMode.DISABLED);
		System.setSecurityManager(flinkUserSecurityManager);
		FlinkUserSecurityManager.monitorSystemExitForCurrentThread();
		assertTrue(flinkUserSecurityManager.systemExitMonitored());
		flinkUserSecurityManager.checkExit(TEST_EXIT_CODE);
	}

	@Test(expected = IllegalConfigurationException.class)
	public void testInvalidConfiguration() {
		Configuration configuration = new Configuration();
		configuration.setString(SecurityOptions.CHECK_SYSTEM_EXIT, "test-invalid");
		flinkUserSecurityManager = FlinkUserSecurityManager.fromConfiguration(configuration);
	}
}
