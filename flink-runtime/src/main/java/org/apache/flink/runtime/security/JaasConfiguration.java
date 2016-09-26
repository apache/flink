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
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * JAAS configuration provider object that provides default LoginModule for various connectors that supports
 * JAAS/SASL based Kerberos authentication. The implementation is inspired from Hadoop UGI class.
 *
 * Different connectors uses different login module name to implement JAAS based authentication support.
 * For example, Kafka expects the login module name to be "kafkaClient" whereas ZooKeeper expect the
 * name to be "client". This sets responsibility on the Flink cluster administrator to configure/provide right
 * JAAS config entries. To simplify this requirement, we have introduced this abstraction that provides
 * a standard lookup to get the login module entry for the JAAS based authentication to work.
 *
 * HDFS connector will not be impacted with this configuration since it uses UGI based mechanism to authenticate.
 *
 * <a href="https://docs.oracle.com/javase/7/docs/api/javax/security/auth/login/Configuration.html">Configuration</a>
 *
 */

@Internal
public class JaasConfiguration extends Configuration {

	private static final Logger LOG = LoggerFactory.getLogger(JaasConfiguration.class);

	public static final String JAVA_VENDOR_NAME = System.getProperty("java.vendor");

	public static final boolean IBM_JAVA;

	private static final Map<String, String> debugOptions = new HashMap<>();

	private static final Map<String, String> kerberosCacheOptions = new HashMap<>();

	private static final Map<String, String> keytabKerberosOptions = new HashMap<>();

	private static final AppConfigurationEntry userKerberosAce;

	private AppConfigurationEntry keytabKerberosAce = null;

	static {

		IBM_JAVA = JAVA_VENDOR_NAME.contains("IBM");

		if(LOG.isDebugEnabled()) {
			debugOptions.put("debug", "true");
		}

		if(IBM_JAVA) {
			kerberosCacheOptions.put("useDefaultCcache", "true");
		} else {
			kerberosCacheOptions.put("doNotPrompt", "true");
			kerberosCacheOptions.put("useTicketCache", "true");
		}

		String ticketCache = System.getenv("KRB5CCNAME");
		if(ticketCache != null) {
			if(IBM_JAVA) {
				System.setProperty("KRB5CCNAME", ticketCache);
			} else {
				kerberosCacheOptions.put("ticketCache", ticketCache);
			}
		}

		kerberosCacheOptions.put("renewTGT", "true");
		kerberosCacheOptions.putAll(debugOptions);

		userKerberosAce = new AppConfigurationEntry(
				KerberosUtil.getKrb5LoginModuleName(),
				AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL,
				kerberosCacheOptions);

	}

	protected JaasConfiguration(String keytab, String principal) {

		LOG.info("Initializing JAAS configuration instance. Parameters: {}, {}", keytab, principal);

		if(StringUtils.isBlank(keytab) && !StringUtils.isBlank(principal) ||
				(!StringUtils.isBlank(keytab) && StringUtils.isBlank(principal))){
			throw new RuntimeException("Both keytab and principal are required and cannot be empty");
		}

		if(!StringUtils.isBlank(keytab) && !StringUtils.isBlank(principal)) {

			if(IBM_JAVA) {
				keytabKerberosOptions.put("useKeytab", prependFileUri(keytab));
				keytabKerberosOptions.put("credsType", "both");
			} else {
				keytabKerberosOptions.put("keyTab", keytab);
				keytabKerberosOptions.put("doNotPrompt", "true");
				keytabKerberosOptions.put("useKeyTab", "true");
				keytabKerberosOptions.put("storeKey", "true");
			}

			keytabKerberosOptions.put("principal", principal);
			keytabKerberosOptions.put("refreshKrb5Config", "true");
			keytabKerberosOptions.putAll(debugOptions);

			keytabKerberosAce = new AppConfigurationEntry(
					KerberosUtil.getKrb5LoginModuleName(),
					AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
					keytabKerberosOptions);
		}
	}

	public static Map<String, String> getKeytabKerberosOptions() {
		return keytabKerberosOptions;
	}

	private static String prependFileUri(String keytabPath) {
		File f = new File(keytabPath);
		return f.toURI().toString();
	}

	@Override
	public AppConfigurationEntry[] getAppConfigurationEntry(String applicationName) {

		LOG.debug("JAAS configuration requested for the application entry: {}", applicationName);

		AppConfigurationEntry[] appConfigurationEntry;

		if(keytabKerberosAce != null) {
			appConfigurationEntry = new AppConfigurationEntry[] {keytabKerberosAce, userKerberosAce};
		} else {
			appConfigurationEntry = new AppConfigurationEntry[] {userKerberosAce};
		}

		return appConfigurationEntry;
	}

}