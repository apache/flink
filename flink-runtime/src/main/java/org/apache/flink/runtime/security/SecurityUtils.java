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

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;

/*
 * Utils for configuring security. The following security mechanism are supported:
 *
 * 1. Java Authentication and Authorization Service (JAAS)
 * 2. Hadoop's User Group Information (UGI)
 */
public class SecurityUtils {

	private static final Logger LOG = LoggerFactory.getLogger(SecurityUtils.class);

	public static final String JAAS_CONF_FILENAME = "flink-jaas.conf";

	public static final String JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";

	private static final String ZOOKEEPER_SASL_CLIENT = "zookeeper.sasl.client";

	private static final String ZOOKEEPER_SASL_CLIENT_USERNAME = "zookeeper.sasl.client.username";

	private static SecurityContext installedContext = new NoOpSecurityContext();

	public static SecurityContext getInstalledContext() { return installedContext; }

	/**
	 * Performs a static initialization of the JAAS and Hadoop UGI security mechanism.
	 * It creates the in-memory JAAS configuration object which will serve appropriate
	 * ApplicationConfigurationEntry for the connector login module implementation that
	 * authenticates Kerberos identity using SASL/JAAS based mechanism.
	 */
	public static void install(SecurityConfiguration config) throws Exception {

		if (!config.securityIsEnabled()) {
			// do not perform any initialization if no Kerberos crendetails are provided
			return;
		}

		// establish the JAAS config
		JaasConfiguration jaasConfig = new JaasConfiguration(config.keytab, config.principal);
		javax.security.auth.login.Configuration.setConfiguration(jaasConfig);

		populateSystemSecurityProperties(config.flinkConf);

		// establish the UGI login user
		UserGroupInformation.setConfiguration(config.hadoopConf);

		// only configure Hadoop security if we have security enabled
		if (UserGroupInformation.isSecurityEnabled()) {

			final UserGroupInformation loginUser;

			if (config.keytab != null && !StringUtils.isBlank(config.principal)) {
				String keytabPath = (new File(config.keytab)).getAbsolutePath();

				UserGroupInformation.loginUserFromKeytab(config.principal, keytabPath);

				loginUser = UserGroupInformation.getLoginUser();

				// supplement with any available tokens
				String fileLocation = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
				if (fileLocation != null) {
				/*
				 * Use reflection API since the API semantics are not available in Hadoop1 profile. Below APIs are
				 * used in the context of reading the stored tokens from UGI.
				 * Credentials cred = Credentials.readTokenStorageFile(new File(fileLocation), config.hadoopConf);
				 * loginUser.addCredentials(cred);
				*/
					try {
						Method readTokenStorageFileMethod = Credentials.class.getMethod("readTokenStorageFile",
							File.class, org.apache.hadoop.conf.Configuration.class);
						Credentials cred = (Credentials) readTokenStorageFileMethod.invoke(null, new File(fileLocation),
							config.hadoopConf);
						Method addCredentialsMethod = UserGroupInformation.class.getMethod("addCredentials",
							Credentials.class);
						addCredentialsMethod.invoke(loginUser, cred);
					} catch (NoSuchMethodException e) {
						LOG.warn("Could not find method implementations in the shaded jar. Exception: {}", e);
					}
				}
			} else {
				// login with current user credentials (e.g. ticket cache)
				try {
					//Use reflection API to get the login user object
					//UserGroupInformation.loginUserFromSubject(null);
					Method loginUserFromSubjectMethod = UserGroupInformation.class.getMethod("loginUserFromSubject", Subject.class);
					Subject subject = null;
					loginUserFromSubjectMethod.invoke(null, subject);
				} catch (NoSuchMethodException e) {
					LOG.warn("Could not find method implementations in the shaded jar. Exception: {}", e);
				}

				// note that the stored tokens are read automatically
				loginUser = UserGroupInformation.getLoginUser();
			}

			LOG.info("Hadoop user set to {}", loginUser.toString());

			boolean delegationToken = false;
			final Text HDFS_DELEGATION_KIND = new Text("HDFS_DELEGATION_TOKEN");
			Collection<Token<? extends TokenIdentifier>> usrTok = loginUser.getTokens();
			for (Token<? extends TokenIdentifier> token : usrTok) {
				final Text id = new Text(token.getIdentifier());
				LOG.debug("Found user token " + id + " with " + token);
				if (token.getKind().equals(HDFS_DELEGATION_KIND)) {
					delegationToken = true;
				}
			}

			if (!loginUser.hasKerberosCredentials()) {
				//throw an error in non-yarn deployment if kerberos cache is not available
				if (!delegationToken) {
					LOG.error("Hadoop Security is enabled but current login user does not have Kerberos Credentials");
					throw new RuntimeException("Hadoop Security is enabled but current login user does not have Kerberos Credentials");
				}
			}

			if (!(installedContext instanceof NoOpSecurityContext)) {
				LOG.warn("overriding previous security context");
			}

			installedContext = new HadoopSecurityContext(loginUser);
		}
	}

	static void clearContext() {
		installedContext = new NoOpSecurityContext();
	}

	/*
	 * This method configures some of the system properties that are require for ZK and Kafka SASL authentication
	 * See: https://github.com/apache/kafka/blob/0.9.0/clients/src/main/java/org/apache/kafka/common/security/kerberos/Login.java#L289
	 * See: https://github.com/sgroschupf/zkclient/blob/master/src/main/java/org/I0Itec/zkclient/ZkClient.java#L900
	 * In this method, setting java.security.auth.login.config configuration is configured only to support ZK and
	 * Kafka current code behavior.
	 */
	private static void populateSystemSecurityProperties(Configuration configuration) {
		Preconditions.checkNotNull(configuration, "The supplied configuration was null");

		boolean disableSaslClient = configuration.getBoolean(HighAvailabilityOptions.ZOOKEEPER_SASL_DISABLE);

		if (disableSaslClient) {
			LOG.info("SASL client auth for ZK will be disabled");
			//SASL auth is disabled by default but will be enabled if specified in configuration
			System.setProperty(ZOOKEEPER_SASL_CLIENT,"false");
			return;
		}

		// load Jaas config file to initialize SASL
		final File jaasConfFile;
		try {
			Path jaasConfPath = Files.createTempFile(JAAS_CONF_FILENAME, "");
			InputStream jaasConfStream = SecurityUtils.class.getClassLoader().getResourceAsStream(JAAS_CONF_FILENAME);
			Files.copy(jaasConfStream, jaasConfPath, StandardCopyOption.REPLACE_EXISTING);
			jaasConfFile = jaasConfPath.toFile();
			jaasConfFile.deleteOnExit();
			jaasConfStream.close();
		} catch (IOException e) {
			throw new RuntimeException("SASL auth is enabled for ZK but unable to " +
				"locate pseudo Jaas config provided with Flink", e);
		}

		LOG.info("Enabling {} property with pseudo JAAS config file: {}",
				JAVA_SECURITY_AUTH_LOGIN_CONFIG, jaasConfFile.getAbsolutePath());

		//ZK client module lookup the configuration to handle SASL.
		//https://github.com/sgroschupf/zkclient/blob/master/src/main/java/org/I0Itec/zkclient/ZkClient.java#L900
		System.setProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, jaasConfFile.getAbsolutePath());
		System.setProperty(ZOOKEEPER_SASL_CLIENT, "true");

		String zkSaslServiceName = configuration.getValue(HighAvailabilityOptions.ZOOKEEPER_SASL_SERVICE_NAME);
		if (!StringUtils.isBlank(zkSaslServiceName)) {
			LOG.info("ZK SASL service name: {} is provided in the configuration", zkSaslServiceName);
			System.setProperty(ZOOKEEPER_SASL_CLIENT_USERNAME, zkSaslServiceName);
		}

	}

	/**
	 * Inputs for establishing the security context.
	 */
	public static class SecurityConfiguration {

		private Configuration flinkConf;

		private org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

		private String keytab;

		private String principal;

		public String getKeytab() {
			return keytab;
		}

		public String getPrincipal() {
			return principal;
		}

		public SecurityConfiguration(Configuration flinkConf) {
			this.flinkConf = flinkConf;

			String keytab = flinkConf.getString(ConfigConstants.SECURITY_KEYTAB_KEY, null);
			String principal = flinkConf.getString(ConfigConstants.SECURITY_PRINCIPAL_KEY, null);
			validate(keytab, principal);

			this.keytab = keytab;
			this.principal = principal;
		}

		public SecurityConfiguration setHadoopConfiguration(org.apache.hadoop.conf.Configuration conf) {
			this.hadoopConf = conf;
			return this;
		}

		private void validate(String keytab, String principal) {
			LOG.debug("keytab {} and principal {} .", keytab, principal);

			if(StringUtils.isBlank(keytab) && !StringUtils.isBlank(principal) ||
					!StringUtils.isBlank(keytab) && StringUtils.isBlank(principal)) {
				if(StringUtils.isBlank(keytab)) {
					LOG.warn("Keytab is null or empty");
				}
				if(StringUtils.isBlank(principal)) {
					LOG.warn("Principal is null or empty");
				}
				throw new RuntimeException("Requires both keytab and principal to be provided");
			}

			if(!StringUtils.isBlank(keytab)) {
				File keytabFile = new File(keytab);
				if(!keytabFile.exists() || !keytabFile.isFile()) {
					LOG.warn("Not a valid keytab: {} file", keytab);
					throw new RuntimeException("Invalid keytab file: " + keytab + " passed");
				}
			}

		}

		public boolean securityIsEnabled() {
			return keytab != null && principal != null;
		}
	}

	// Just a util, shouldn't be instantiated.
	private SecurityUtils() {}

}
