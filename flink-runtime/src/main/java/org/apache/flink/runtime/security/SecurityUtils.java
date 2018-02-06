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

import org.apache.flink.runtime.security.modules.SecurityModule;
import org.apache.flink.runtime.security.modules.SecurityModuleFactory;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Utils for configuring security. The following security subsystems are supported:
 * 1. Java Authentication and Authorization Service (JAAS)
 * 2. Hadoop's User Group Information (UGI)
 * 3. ZooKeeper's process-wide security settings.
 */
public class SecurityUtils {

	private static final Logger LOG = LoggerFactory.getLogger(SecurityUtils.class);

	private static SecurityContext installedContext = new NoOpSecurityContext();

	private static List<SecurityModule> installedModules = null;

	public static SecurityContext getInstalledContext() {
		return installedContext;
	}

	public static List<SecurityModule> getInstalledModules() {
		return installedModules;
	}

	/**
	 * Installs a process-wide security configuration.
	 *
	 * <p>Applies the configuration using the available security modules (i.e. Hadoop, JAAS).
	 */
	public static void install(SecurityConfiguration config) throws Exception {

		// install the security modules
		List<SecurityModule> modules = new ArrayList<>();
		try {
			for (SecurityModuleFactory moduleFactory : config.getSecurityModuleFactories()) {
				SecurityModule module = moduleFactory.createModule(config);
				// can be null if a SecurityModule is not supported in the current environment
				if (module != null) {
					module.install();
					modules.add(module);
				}
			}
		}
		catch (Exception ex) {
			throw new Exception("unable to establish the security context", ex);
		}
		installedModules = modules;

		// First check if we have Hadoop in the ClassPath. If not, we simply don't do anything.
		try {
			Class.forName(
				"org.apache.hadoop.security.UserGroupInformation",
				false,
				SecurityUtils.class.getClassLoader());

			// install a security context
			// use the Hadoop login user as the subject of the installed security context
			if (!(installedContext instanceof NoOpSecurityContext)) {
				LOG.warn("overriding previous security context");
			}
			UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
			installedContext = new HadoopSecurityContext(loginUser);
		} catch (ClassNotFoundException e) {
			LOG.info("Cannot install HadoopSecurityContext because Hadoop cannot be found in the Classpath.");
		} catch (LinkageError e) {
			LOG.error("Cannot install HadoopSecurityContext.", e);
		}
	}

	static void uninstall() {
		if (installedModules != null) {
			// uninstall them in reverse order
			for (int i = installedModules.size() - 1; i >= 0; i--) {
				SecurityModule module = installedModules.get(i);
				try {
					module.uninstall();
				}
				catch (UnsupportedOperationException ignored) {
				}
				catch (SecurityModule.SecurityInstallException e) {
					LOG.warn("unable to uninstall a security module", e);
				}
			}
			installedModules = null;
		}

		installedContext = new NoOpSecurityContext();
	}

	// Just a util, shouldn't be instantiated.
	private SecurityUtils() {}

}
