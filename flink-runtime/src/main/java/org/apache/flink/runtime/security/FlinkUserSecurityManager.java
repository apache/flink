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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Permission;

/**
 * Flink user security manager to control unexpected user behaviors that potentially impact cluster availability, for
 * example, it can warn or prevent user code from terminating JVM by System.exit or halt by logging or throwing an exception.
 * This does not necessarily prevent malicious users who try to tweak security manager on their own, but more for being dependable
 * against user mistakes by gracefully handling them informing users rather than causing silent unavailability.
 */
public class FlinkUserSecurityManager extends SecurityManager {

	/**
	 * The mode of how to handle user code attempting to exit JVM.
	 */
	public enum CheckExitMode {
		/** No check is enabled, that is allowing exit without any action. */
		DISABLED,
		/** Warn by logging but still allowing exit to be performed. */
		WARN,
		/** Throw exception when exit is attempted disallowing JVM termination. */
		THROW,
	}

	static final Logger LOG = LoggerFactory.getLogger(FlinkUserSecurityManager.class);

	private final SecurityManager originalSecurityManager = System.getSecurityManager();
	private ThreadLocal<Boolean> monitorSystemExit = new InheritableThreadLocal<>();
	private CheckExitMode checkExitMode;

	public FlinkUserSecurityManager(CheckExitMode checkExitMode) {
		super();
		this.checkExitMode = checkExitMode;

		LOG.info("FlinkUserSecurityManager is created with {} system exit check (previous security manager is {})",
			this.checkExitMode, originalSecurityManager != null ? originalSecurityManager : "not existing");
	}

	/**
	 * Instantiate FlinkUserSecurityManager from configuration. Return null if no security manager
	 * check is needed, so that a caller can skip setting security manager avoiding runtime check cost,
	 * if there is no security check set up already. Use {@link #setFromConfiguration} helper, which
	 * handles disabled case.
	 *
	 * @param configuration to instantiate the security manager from
	 *
	 * @return FlinkUserSecurityManager instantiated baesd on configuration. Return null if disabled.
	 */
	public static FlinkUserSecurityManager fromConfiguration(Configuration configuration) {
		final String checkExitModeConfig = configuration.getString(SecurityOptions.CHECK_SYSTEM_EXIT);
		final CheckExitMode checkExitMode;

		try {
			checkExitMode = CheckExitMode.valueOf(checkExitModeConfig.toUpperCase());
		} catch (Exception ex) {
			throw new IllegalConfigurationException(
				String.format("%s is invalid configuration for %s.", checkExitModeConfig, SecurityOptions.CHECK_SYSTEM_EXIT.key()),
				ex);
		}

		// If no check is enabled, return null so that caller can avoid setting security manager not to incur any runtime cost.
		if (checkExitMode == CheckExitMode.DISABLED) {
			return null;
		}
		// Add more configuration parameters that need user security manager (currently only for system exit).
		return new FlinkUserSecurityManager(checkExitMode);
	}

	public static void setFromConfiguration(Configuration configuration) {
		final FlinkUserSecurityManager flinkUserSecurityManager =
			FlinkUserSecurityManager.fromConfiguration(configuration);
		if (flinkUserSecurityManager != null) {
			System.setSecurityManager(flinkUserSecurityManager);
		}
	}

	public void checkPermission(Permission perm) {
		if (originalSecurityManager != null) {
			originalSecurityManager.checkPermission(perm);
		}
	}

	public void checkPermission(Permission perm, Object context) {
		if (originalSecurityManager != null) {
			originalSecurityManager.checkPermission(perm, context);
		}
	}

	public void checkExit(int status) {
		super.checkExit(status);
		if (!systemExitMonitored()) {
			return;
		}
		switch (checkExitMode) {
			case DISABLED:
				break;
			case WARN:
				// Add exception trace log to help users to debug where exit came from.
				LOG.warn("Exiting JVM with status {} is monitored, logging and exiting",
					status, new UserSystemExitException());
				break;
			case THROW:
				throw new UserSystemExitException();
			default:
				// Must not happen if exhaustively handling all modes above. Logging as being already at exit path.
				LOG.warn("No valid check exit mode configured: {}", checkExitMode);
		}
	}

	public SecurityManager getOriginalSecurityManager() {
		return originalSecurityManager;
	}

	public void monitorSystemExit() {
		monitorSystemExit.set(true);
	}

	public void unmonitorSystemExit() {
		monitorSystemExit.set(false);
	}

	public boolean systemExitMonitored() {
		return Boolean.TRUE.equals(monitorSystemExit.get());
	}

	public static void monitorSystemExitForCurrentThread() {
		SecurityManager securityManager = System.getSecurityManager();
		if (securityManager instanceof  FlinkUserSecurityManager) {
			((FlinkUserSecurityManager) securityManager).monitorSystemExit();
		}
	}

	public static void unmonitorSystemExitForCurrentThread() {
		SecurityManager securityManager = System.getSecurityManager();
		if (securityManager instanceof FlinkUserSecurityManager) {
			((FlinkUserSecurityManager) securityManager).unmonitorSystemExit();
		}
	}
}
