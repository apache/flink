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

import org.apache.flink.runtime.security.contexts.NoOpSecurityContext;
import org.apache.flink.runtime.security.contexts.SecurityContext;
import org.apache.flink.runtime.security.factories.SecurityContextFactory;
import org.apache.flink.runtime.security.factories.SecurityFactoryService;
import org.apache.flink.runtime.security.factories.SecurityModuleFactory;
import org.apache.flink.runtime.security.modules.SecurityModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Security Environment that holds the security context and modules installed.
 */
public class SecurityEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(SecurityEnvironment.class);

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
		// Install the security modules first before installing the security context
		installModules(config);
		installContext(config);
	}

	static void installModules(SecurityConfiguration config) throws Exception {

		// install the security module factories
		List<SecurityModule> modules = new ArrayList<>();
		try {
			for (String moduleFactoryClass : config.getSecurityModuleFactories()) {
				SecurityModuleFactory moduleFactory = SecurityFactoryService.findModuleFactory(moduleFactoryClass);
				if (moduleFactory == null) {
					throw new Exception("unable to local security module factory for: " + moduleFactoryClass);
				}
				SecurityModule module = moduleFactory.createModule(config);
				// can be null if a SecurityModule is not supported in the current environment
				if (module != null) {
					module.install();
					modules.add(module);
				}
			}
		} catch (Exception ex) {
			throw new Exception("unable to install all security modules", ex);
		}
		installedModules = modules;
	}

	static void installContext(SecurityConfiguration config) throws Exception {
		// install the security context factory
		String contextFactoryClass = config.getSecurityContextFactory();
		SecurityContextFactory contextFactory = SecurityFactoryService.findContextFactory(contextFactoryClass);
		if (contextFactory == null) {
			throw new Exception("unable to local security context factory for: " + contextFactoryClass);
		}
		installedContext = contextFactory.createContext(config);
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
}
