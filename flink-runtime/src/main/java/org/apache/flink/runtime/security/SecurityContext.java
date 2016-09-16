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
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.File;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;

/*
 * Process-wide security context object which initializes UGI with appropriate security credentials and also it
 * creates in-memory JAAS configuration object which will serve appropriate ApplicationConfigurationEntry for the
 * connector login module implementation that authenticates Kerberos identity using SASL/JAAS based mechanism.
 */
@Internal
public class SecurityContext {

	private static final Logger LOG = LoggerFactory.getLogger(SecurityContext.class);

	public static final String JAAS_CONF_FILENAME = "flink-jaas.conf";

	private static final String JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";

	private static final String ZOOKEEPER_SASL_CLIENT = "zookeeper.sasl.client";

	private static final String ZOOKEEPER_SASL_CLIENT_USERNAME = "zookeeper.sasl.client.username";

	private static SecurityContext installedContext;

	public static SecurityContext getInstalled() { return installedContext; }

	private UserGroupInformation ugi;

	SecurityContext(UserGroupInformation ugi) {
		if(ugi == null) {
			throw new RuntimeException("UGI passed cannot be null");
		}
		this.ugi = ugi;
	}

	public <T> T runSecured(final FlinkSecuredRunner<T> runner) throws Exception {
		return ugi.doAs(new PrivilegedExceptionAction<T>() {
			@Override
			public T run() throws Exception {
				return runner.run();
			}
		});
	}

	public static void install(SecurityConfiguration config) throws Exception {

		// perform static initialization of UGI, JAAS
		if(installedContext != null) {
			LOG.warn("overriding previous security context");
		}

		// establish the JAAS config
		JaasConfiguration jaasConfig = new JaasConfiguration(config.keytab, config.principal);
		javax.security.auth.login.Configuration.setConfiguration(jaasConfig);

		populateSystemSecurityProperties(config.flinkConf);

		// establish the UGI login user
		UserGroupInformation.setConfiguration(config.hadoopConf);

		UserGroupInformation loginUser;

		if(UserGroupInformation.isSecurityEnabled() &&
				config.keytab != null && !StringUtils.isBlank(config.principal)) {
			String keytabPath = (new File(config.keytab)).getAbsolutePath();

			UserGroupInformation.loginUserFromKeytab(config.principal, keytabPath);

			loginUser = UserGroupInformation.getLoginUser();

			// supplement with any available tokens
			String fileLocation = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
			if(fileLocation != null) {
				/*
				 * Use reflection API since the API semantics are not available in Hadoop1 profile. Below APIs are
				 * used in the context of reading the stored tokens from UGI.
				 * Credentials cred = Credentials.readTokenStorageFile(new File(fileLocation), config.hadoopConf);
				 * loginUser.addCredentials(cred);
				*/
				try {
					Method readTokenStorageFileMethod = Credentials.class.getMethod("readTokenStorageFile",
							File.class, org.apache.hadoop.conf.Configuration.class);
					Credentials cred = (Credentials) readTokenStorageFileMethod.invoke(null,new File(fileLocation),
							config.hadoopConf);
					Method addCredentialsMethod = UserGroupInformation.class.getMethod("addCredentials",
							Credentials.class);
					addCredentialsMethod.invoke(loginUser,cred);
				} catch(NoSuchMethodException e) {
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
				loginUserFromSubjectMethod.invoke(null,subject);
			} catch(NoSuchMethodException e) {
				LOG.warn("Could not find method implementations in the shaded jar. Exception: {}", e);
			}

			loginUser = UserGroupInformation.getLoginUser();
			// note that the stored tokens are read automatically
		}

		boolean delegationToken = false;
		final Text HDFS_DELEGATION_KIND = new Text("HDFS_DELEGATION_TOKEN");
		Collection<Token<? extends TokenIdentifier>> usrTok = loginUser.getTokens();
		for(Token<? extends TokenIdentifier> token : usrTok) {
			final Text id = new Text(token.getIdentifier());
			LOG.debug("Found user token " + id + " with " + token);
			if(token.getKind().equals(HDFS_DELEGATION_KIND)) {
				delegationToken = true;
			}
		}

		if(UserGroupInformation.isSecurityEnabled() && !loginUser.hasKerberosCredentials()) {
			//throw an error in non-yarn deployment if kerberos cache is not available
			if(!delegationToken) {
				LOG.error("Hadoop Security is enabled but current login user does not have Kerberos Credentials");
				throw new RuntimeException("Hadoop Security is enabled but current login user does not have Kerberos Credentials");
			}
		}

		installedContext = new SecurityContext(loginUser);
	}

	/*
	 * This method configures some of the system properties that are require for ZK and Kafka SASL authentication
	 * See: https://github.com/apache/kafka/blob/0.9.0/clients/src/main/java/org/apache/kafka/common/security/kerberos/Login.java#L289
	 * See: https://github.com/sgroschupf/zkclient/blob/master/src/main/java/org/I0Itec/zkclient/ZkClient.java#L900
	 * In this method, setting java.security.auth.login.config configuration is configured only to support ZK and
	 * Kafka current code behavior.
	 */
	private static void populateSystemSecurityProperties(Configuration configuration) {

		//required to be empty for Kafka but we will override the property
		//with pseudo JAAS configuration file if SASL auth is enabled for ZK
		System.setProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, "");

		if(configuration == null) {
			return;
		}

		boolean disableSaslClient = configuration.getBoolean(ConfigConstants.ZOOKEEPER_SASL_DISABLE,
				ConfigConstants.DEFAULT_ZOOKEEPER_SASL_DISABLE);
		if(disableSaslClient) {
			LOG.info("SASL client auth for ZK will be disabled");
			//SASL auth is disabled by default but will be enabled if specified in configuration
			System.setProperty(ZOOKEEPER_SASL_CLIENT,"false");
			return;
		}

		String baseDir = configuration.getString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, null);
		if(baseDir == null) {
			String message = "SASL auth is enabled for ZK but unable to locate pseudo Jaas config " +
					"since " + ConfigConstants.FLINK_BASE_DIR_PATH_KEY + " is not provided";
			LOG.error(message);
			throw new IllegalConfigurationException(message);
		}

		File f = new File(baseDir);
		if(!f.exists() || !f.isDirectory()) {
			LOG.error("Invalid flink base directory {} configuration provided", baseDir);
			throw new IllegalConfigurationException("Invalid flink base directory configuration provided");
		}

		File jaasConfigFile = new File(f, JAAS_CONF_FILENAME);

		if (!jaasConfigFile.exists() || !jaasConfigFile.isFile()) {

			//check if there is a conf directory
			File confDir = new File(f, "conf");
			if(!confDir.exists() || !confDir.isDirectory()) {
				LOG.error("Could not locate " + JAAS_CONF_FILENAME);
				throw new IllegalConfigurationException("Could not locate " + JAAS_CONF_FILENAME);
			}

			jaasConfigFile = new File(confDir, JAAS_CONF_FILENAME);

			if (!jaasConfigFile.exists() || !jaasConfigFile.isFile()) {
				LOG.error("Could not locate " + JAAS_CONF_FILENAME);
				throw new IllegalConfigurationException("Could not locate " + JAAS_CONF_FILENAME);
			}
		}

		LOG.info("Enabling {} property with pseudo JAAS config file: {}",
				JAVA_SECURITY_AUTH_LOGIN_CONFIG, jaasConfigFile);

		//ZK client module lookup the configuration to handle SASL.
		//https://github.com/sgroschupf/zkclient/blob/master/src/main/java/org/I0Itec/zkclient/ZkClient.java#L900
		System.setProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, jaasConfigFile.getAbsolutePath());
		System.setProperty(ZOOKEEPER_SASL_CLIENT,"true");

		String zkSaslServiceName = configuration.getString(ConfigConstants.ZOOKEEPER_SASL_SERVICE_NAME, null);
		if(!StringUtils.isBlank(zkSaslServiceName)) {
			LOG.info("ZK SASL service name: {} is provided in the configuration", zkSaslServiceName);
			System.setProperty(ZOOKEEPER_SASL_CLIENT_USERNAME,zkSaslServiceName);
		}

	}

	/**
	 * Inputs for establishing the security context.
	 */
	public static class SecurityConfiguration {

		Configuration flinkConf;

		org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

		String keytab;

		String principal;

		public String getKeytab() {
			return keytab;
		}

		public String getPrincipal() {
			return principal;
		}

		public SecurityConfiguration setFlinkConfiguration(Configuration flinkConf) {

			this.flinkConf = flinkConf;

			String keytab = flinkConf.getString(ConfigConstants.SECURITY_KEYTAB_KEY, null);

			String principal = flinkConf.getString(ConfigConstants.SECURITY_PRINCIPAL_KEY, null);

			validate(keytab, principal);

			LOG.debug("keytab {} and principal {} .", keytab, principal);

			this.keytab = keytab;

			this.principal = principal;

			return this;
		}

		public SecurityConfiguration setHadoopConfiguration(org.apache.hadoop.conf.Configuration conf) {
			this.hadoopConf = conf;
			return this;
		}

		private void validate(String keytab, String principal) {

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
	}

	public interface FlinkSecuredRunner<T> {
		T run() throws Exception;
	}

}