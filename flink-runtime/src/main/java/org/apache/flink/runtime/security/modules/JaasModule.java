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

package org.apache.flink.runtime.security.modules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.security.DynamicConfiguration;
import org.apache.flink.runtime.security.KerberosUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Responsible for installing a process-wide JAAS configuration.
 *
 * <p>The installed configuration combines login modules based on:
 * - the user-supplied JAAS configuration file, if any
 * - a Kerberos keytab, if configured
 * - any cached Kerberos credentials from the current environment
 *
 * <p>The module also installs a default JAAS config file (if necessary) for
 * compatibility with ZK and Kafka.  Note that the JRE actually draws on numerous file locations.
 * See: https://docs.oracle.com/javase/7/docs/jre/api/security/jaas/spec/com/sun/security/auth/login/ConfigFile.html
 * See: https://github.com/apache/kafka/blob/0.9.0/clients/src/main/java/org/apache/kafka/common/security/kerberos/Login.java#L289
 */
@Internal
public class JaasModule implements SecurityModule {

	private static final Logger LOG = LoggerFactory.getLogger(JaasModule.class);

	static final String JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";

	static final String JAAS_CONF_RESOURCE_NAME = "flink-jaas.conf";

	private final SecurityConfiguration securityConfig;

	private String priorConfigFile;
	private javax.security.auth.login.Configuration priorConfig;

	private DynamicConfiguration currentConfig;

	public JaasModule(SecurityConfiguration securityConfig) {
		this.securityConfig = checkNotNull(securityConfig);
	}

	@Override
	public void install() throws SecurityInstallException {

		// ensure that a config file is always defined, for compatibility with
		// ZK and Kafka which check for the system property and existence of the file
		priorConfigFile = System.getProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, null);
		if (priorConfigFile == null) {
			File configFile = generateDefaultConfigFile();
			System.setProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, configFile.getAbsolutePath());
		}

		// read the JAAS configuration file
		priorConfig = javax.security.auth.login.Configuration.getConfiguration();

		// construct a dynamic JAAS configuration
		currentConfig = new DynamicConfiguration(priorConfig);

		// wire up the configured JAAS login contexts to use the krb5 entries
		AppConfigurationEntry[] krb5Entries = getAppConfigurationEntries(securityConfig);
		if (krb5Entries != null) {
			for (String app : securityConfig.getLoginContextNames()) {
				currentConfig.addAppConfigurationEntry(app, krb5Entries);
			}
		}

		javax.security.auth.login.Configuration.setConfiguration(currentConfig);
	}

	@Override
	public void uninstall() throws SecurityInstallException {
		if (priorConfigFile != null) {
			System.setProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, priorConfigFile);
		} else {
			System.clearProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG);
		}
		javax.security.auth.login.Configuration.setConfiguration(priorConfig);
	}

	public DynamicConfiguration getCurrentConfiguration() {
		return currentConfig;
	}

	private static AppConfigurationEntry[] getAppConfigurationEntries(SecurityConfiguration securityConfig) {

		AppConfigurationEntry userKerberosAce = null;
		if (securityConfig.useTicketCache()) {
			userKerberosAce = KerberosUtils.ticketCacheEntry();
		}
		AppConfigurationEntry keytabKerberosAce = null;
		if (securityConfig.getKeytab() != null) {
			keytabKerberosAce = KerberosUtils.keytabEntry(securityConfig.getKeytab(), securityConfig.getPrincipal());
		}

		AppConfigurationEntry[] appConfigurationEntry;
		if (userKerberosAce != null && keytabKerberosAce != null) {
			appConfigurationEntry = new AppConfigurationEntry[]{keytabKerberosAce, userKerberosAce};
		} else if (keytabKerberosAce != null) {
			appConfigurationEntry = new AppConfigurationEntry[]{keytabKerberosAce};
		} else if (userKerberosAce != null) {
			appConfigurationEntry = new AppConfigurationEntry[]{userKerberosAce};
		} else {
			return null;
		}

		return appConfigurationEntry;
	}

	/**
	 * Generate the default JAAS config file.
	 */
	private static File generateDefaultConfigFile() {
		final File jaasConfFile;
		try {
			Path jaasConfPath = Files.createTempFile("jaas-", ".conf");
			try (InputStream resourceStream = JaasModule.class.getClassLoader().getResourceAsStream(JAAS_CONF_RESOURCE_NAME)) {
				Files.copy(resourceStream, jaasConfPath, StandardCopyOption.REPLACE_EXISTING);
			}
			jaasConfFile = jaasConfPath.toFile();
			jaasConfFile.deleteOnExit();
		} catch (IOException e) {
			throw new RuntimeException("unable to generate a JAAS configuration file", e);
		}
		return jaasConfFile;
	}
}
