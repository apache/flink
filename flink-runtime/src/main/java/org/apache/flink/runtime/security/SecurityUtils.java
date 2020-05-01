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
import org.apache.flink.runtime.security.contexts.SecurityContextFactory;
import org.apache.flink.runtime.security.modules.SecurityModule;
import org.apache.flink.runtime.security.modules.SecurityModuleFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Security Environment that holds the security context and modules installed.
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
		// Install the security modules first before installing the security context
		installModules(config);
		installContext(config);
	}

	static void installModules(SecurityConfiguration config) throws Exception {

		// install the security module factories
		List<SecurityModule> modules = new ArrayList<>();
		for (String moduleFactoryClass : config.getSecurityModuleFactories()) {
			SecurityModuleFactory moduleFactory = null;
			try {
				moduleFactory = SecurityFactoryServiceLoader.findModuleFactory(moduleFactoryClass);
			} catch (NoMatchSecurityFactoryException ne) {
				LOG.error("Unable to instantiate security module factory {}", moduleFactoryClass);
				throw new IllegalArgumentException("Unable to find module factory class", ne);
			}
			SecurityModule module = moduleFactory.createModule(config);
			// can be null if a SecurityModule is not supported in the current environment
			if (module != null) {
				module.install();
				modules.add(module);
			}
		}
		installedModules = modules;
	}

	static void installContext(SecurityConfiguration config) throws Exception {
		// install the security context factory
		for (String contextFactoryClass : config.getSecurityContextFactories()) {
			try {
				SecurityContextFactory contextFactory = SecurityFactoryServiceLoader.findContextFactory(contextFactoryClass);
				if (contextFactory.isCompatibleWith(config)) {
					try {
						installedContext = contextFactory.createContext(config);
						// install the first context that's compatible and ignore the remaining.
						break;
					} catch (SecurityContextInitializeException e) {
						LOG.error("Cannot instantiate security context with: " + contextFactoryClass, e);
					} catch (LinkageError le) {
						LOG.error("Error occur when instantiate security context with: " + contextFactoryClass , le);
					}
				} else {
					LOG.warn("Unable to install incompatible security context factory {}", contextFactoryClass);
				}
			} catch (NoMatchSecurityFactoryException ne) {
				LOG.warn("Unable to instantiate security context factory {}", contextFactoryClass);
			}
		}
		if (installedContext == null) {
			LOG.error("Unable to install a valid security context factory!");
			throw new Exception("Unable to install a valid security context factory!");
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
}
