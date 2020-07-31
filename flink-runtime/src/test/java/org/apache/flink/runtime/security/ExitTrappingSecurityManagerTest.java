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

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.Permission;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Unit tests for {@link ExitTrappingSecurityManager}. */
public class ExitTrappingSecurityManagerTest extends TestLogger {

	private SecurityManager existingManager;

	@Before
	public void before() {
		existingManager = System.getSecurityManager();
	}

	@After
	public void after() {
		System.setSecurityManager(existingManager);
	}

	@Test
	public void testExitWithNoExistingSecurityManager() {
		AtomicInteger customExitExecuted = new AtomicInteger(0);
		ExitTrappingSecurityManager exitTrappingSecurityManager =
			new ExitTrappingSecurityManager(customExitExecuted::set, null);

		exitTrappingSecurityManager.checkExit(42);

		assertThat(customExitExecuted.get(), is(42));
	}

	@Test
	public void testExistingSecurityManagerRespected() {
		SecurityManager securityManager = new SecurityManager() {
			@Override
			public void checkPermission(Permission perm) {
				throw new SecurityException("not allowed");
			}
		};

		ExitTrappingSecurityManager exitTrappingSecurityManager =
			new ExitTrappingSecurityManager(status -> Assert.fail(), securityManager);

		assertThrows("not allowed", SecurityException.class, () -> {
			exitTrappingSecurityManager.checkExit(42);
			return null;
		});
	}

	@Test
	public void testExitCodeHandling() {
		AtomicInteger exitingSecurityManagerCalled = new AtomicInteger(0);
		SecurityManager securityManager = new SecurityManager() {
			@Override
			public void checkExit(int status) {
				exitingSecurityManagerCalled.set(status);
			}
		};

		AtomicInteger customExitExecuted = new AtomicInteger(0);
		ExitTrappingSecurityManager exitTrappingSecurityManager =
			new ExitTrappingSecurityManager(customExitExecuted::set, securityManager);

		exitTrappingSecurityManager.checkExit(42);

		assertThat(exitingSecurityManagerCalled.get(), is(42));
		assertThat(customExitExecuted.get(), is(42));
	}

	@Test
	public void testNotRegisteredByDefault() {
		ExitTrappingSecurityManager.replaceGracefulExitWithHaltIfConfigured(new Configuration());

		assertThat(System.getSecurityManager(), not(instanceOf(ExitTrappingSecurityManager.class)));
	}

	@Test
	public void testRegisteredWhenConfigValueSet() {
		Configuration configuration = new Configuration();
		configuration.set(ClusterOptions.HALT_ON_FATAL_ERROR, true);

		ExitTrappingSecurityManager.replaceGracefulExitWithHaltIfConfigured(configuration);

		assertThat(System.getSecurityManager(), is(instanceOf(ExitTrappingSecurityManager.class)));
	}

	@Test
	public void testRegistrationNotAllowedByExistingSecurityManager() {
		Configuration configuration = new Configuration();
		configuration.set(ClusterOptions.HALT_ON_FATAL_ERROR, true);

		System.setSecurityManager(new SecurityManager() {

			private boolean fired;

			@Override
			public void checkPermission(Permission perm) {
				if (!fired && perm.getName().equals("setSecurityManager")) {
					try {
						throw new SecurityException("not allowed");
					} finally {
						// Allow removing this manager again
						fired = true;
					}
				}
			}
		});

		assertThrows("Could not register forceful shutdown handler.", IllegalConfigurationException.class, () -> {
			ExitTrappingSecurityManager.replaceGracefulExitWithHaltIfConfigured(configuration);
			return null;
		});
	}
}
