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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.security.modules.HadoopModule;
import org.apache.flink.runtime.security.modules.JaasModule;
import org.apache.flink.runtime.security.modules.SecurityModule;
import org.apache.flink.runtime.security.modules.ZooKeeperModule;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

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

	@VisibleForTesting
	static List<SecurityModule> getInstalledModules() {
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
			for (Class<? extends SecurityModule> moduleClass : config.getSecurityModules()) {
				SecurityModule module = moduleClass.newInstance();
				module.install(config);
				modules.add(module);
			}
		}
		catch (Exception ex) {
			throw new Exception("unable to establish the security context", ex);
		}
		installedModules = modules;

		// install a security context
		// use the Hadoop login user as the subject of the installed security context
		if (!(installedContext instanceof NoOpSecurityContext)) {
			LOG.warn("overriding previous security context");
		}
		UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
		installedContext = new HadoopSecurityContext(loginUser);
	}

	static void uninstall() {
		if (installedModules != null) {
			for (SecurityModule module : Lists.reverse(installedModules)) {
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

	/**
	 * The global security configuration.
	 *
	 * <p>See {@link SecurityOptions} for corresponding configuration options.
	 */
	public static class SecurityConfiguration {

		private static final List<Class<? extends SecurityModule>> DEFAULT_MODULES = Collections.unmodifiableList(
			Arrays.asList(HadoopModule.class, JaasModule.class, ZooKeeperModule.class));

		private final List<Class<? extends SecurityModule>> securityModules;

		private final org.apache.hadoop.conf.Configuration hadoopConf;

		private final boolean isZkSaslDisable;

		private final boolean useTicketCache;

		private final String keytab;

		private final String principal;

		private final List<String> loginContextNames;

		private final String zkServiceName;

		private final String zkLoginContextName;

		/**
		 * Create a security configuration from the global configuration.
		 * @param flinkConf the Flink global configuration.
         */
		public SecurityConfiguration(Configuration flinkConf) {
			this(flinkConf, HadoopUtils.getHadoopConfiguration(flinkConf));
		}

		/**
		 * Create a security configuration from the global configuration.
		 * @param flinkConf the Flink global configuration.
		 * @param hadoopConf the Hadoop configuration.
		 */
		public SecurityConfiguration(Configuration flinkConf, org.apache.hadoop.conf.Configuration hadoopConf) {
			this(flinkConf, hadoopConf, DEFAULT_MODULES);
		}

		/**
		 * Create a security configuration from the global configuration.
		 * @param flinkConf the Flink global configuration.
		 * @param hadoopConf the Hadoop configuration.
		 * @param securityModules the security modules to apply.
		 */
		public SecurityConfiguration(Configuration flinkConf,
				org.apache.hadoop.conf.Configuration hadoopConf,
				List<? extends Class<? extends SecurityModule>> securityModules) {
			this.hadoopConf = checkNotNull(hadoopConf);
			this.isZkSaslDisable = flinkConf.getBoolean(SecurityOptions.ZOOKEEPER_SASL_DISABLE);
			this.keytab = flinkConf.getString(SecurityOptions.KERBEROS_LOGIN_KEYTAB);
			this.principal = flinkConf.getString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL);
			this.useTicketCache = flinkConf.getBoolean(SecurityOptions.KERBEROS_LOGIN_USETICKETCACHE);
			this.loginContextNames = parseList(flinkConf.getString(SecurityOptions.KERBEROS_LOGIN_CONTEXTS));
			this.zkServiceName = flinkConf.getString(SecurityOptions.ZOOKEEPER_SASL_SERVICE_NAME);
			this.zkLoginContextName = flinkConf.getString(SecurityOptions.ZOOKEEPER_SASL_LOGIN_CONTEXT_NAME);
			this.securityModules = Collections.unmodifiableList(securityModules);

			validate();
		}

		public boolean isZkSaslDisable() {
			return isZkSaslDisable;
		}

		public String getKeytab() {
			return keytab;
		}

		public String getPrincipal() {
			return principal;
		}

		public boolean useTicketCache() {
			return useTicketCache;
		}

		public org.apache.hadoop.conf.Configuration getHadoopConfiguration() {
			return hadoopConf;
		}

		public List<Class<? extends SecurityModule>> getSecurityModules() {
			return securityModules;
		}

		public List<String> getLoginContextNames() {
			return loginContextNames;
		}

		public String getZooKeeperServiceName() {
			return zkServiceName;
		}

		public String getZooKeeperLoginContextName() {
			return zkLoginContextName;
		}

		private void validate() {
			if (!StringUtils.isBlank(keytab)) {
				// principal is required
				if (StringUtils.isBlank(principal)) {
					throw new IllegalConfigurationException("Kerberos login configuration is invalid; keytab requires a principal.");
				}

				// check the keytab is readable
				File keytabFile = new File(keytab);
				if (!keytabFile.exists() || !keytabFile.isFile() || !keytabFile.canRead()) {
					throw new IllegalConfigurationException("Kerberos login configuration is invalid; keytab is unreadable");
				}
			}
		}

		private static List<String> parseList(String value) {
			if (value == null || value.isEmpty()) {
				return Collections.emptyList();
			}

			return Arrays.asList(value
				.trim()
				.replaceAll("(\\s*,+\\s*)+", ",")
				.split(","));
		}
	}

	// Just a util, shouldn't be instantiated.
	private SecurityUtils() {}

}
