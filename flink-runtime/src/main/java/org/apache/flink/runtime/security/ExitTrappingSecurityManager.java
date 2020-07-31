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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.security.Permission;
import java.util.function.Consumer;

import static org.apache.flink.configuration.ClusterOptions.HALT_ON_FATAL_ERROR;

/**
 * A SecurityManager to execute logic on exit requests.
 * */
public final class ExitTrappingSecurityManager extends SecurityManager {

	/** The behavior to execute when the JVM exists. */
	private final Consumer<Integer> onExitBehavior;

	/** An already in-use SecurityManager. */
	@Nullable
	private final SecurityManager existingManager;

	public ExitTrappingSecurityManager(Consumer<Integer> onExitBehavior, @Nullable SecurityManager existingManager) {
		this.onExitBehavior = Preconditions.checkNotNull(onExitBehavior);
		this.existingManager = existingManager;
	}

	@Override
	public void checkExit(int status) {
		// Check if the existing SecurityManager has a policy.
		if (existingManager != null) {
			existingManager.checkExit(status);
		}
		// Unset ourselves to allow exiting.
		System.setSecurityManager(null);
		// Execute the desired behavior, e.g. forceful exit instead of proceeding with System.exit().
		onExitBehavior.accept(status);
	}

	@Override
	public void checkPermission(Permission perm) {
		if (existingManager != null) {
			existingManager.checkPermission(perm);
		}
	}

	/**
	 * If configured, registers a custom SecurityManager which converts graceful exists calls using
	 * {@code System#exit} into forceful exit calls using {@code Runtime#halt}. The latter does not
	 * perform a clean shutdown using the registered shutdown hooks.
	 * <p></p>
	 * This may be configured to prevent deadlocks with Java 8 and the G1 garbage collection,
	 * see https://issues.apache.org/jira/browse/FLINK-16510.
	 *
	 * @param configuration The Flink configuration
	 */
	public static void replaceGracefulExitWithHaltIfConfigured(Configuration configuration) {
		if (configuration.get(HALT_ON_FATAL_ERROR)) {
			SecurityManager forcefulShutdownManager = new ExitTrappingSecurityManager(
				status -> Runtime.getRuntime().halt(status),
				System.getSecurityManager());
			try {
				System.setSecurityManager(forcefulShutdownManager);
			} catch (Exception e) {
				throw new IllegalConfigurationException(
					String.format("Could not register forceful shutdown handler. This feature requires the permission to " +
							"set a SecurityManager. Either update your existing SecurityManager to allow setting a SecurityManager " +
							"or disable this feature by updating your Flink config with the following: " +
							"'%s: %s'",
						HALT_ON_FATAL_ERROR.key(),
						HALT_ON_FATAL_ERROR.defaultValue()),
					e);
			}
		}
	}
}
