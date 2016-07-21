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

package org.apache.flink.test.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.security.JaasConfiguration;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link TestingJaasConfiguration} for handling the integration test case since it requires to manage
 * client principal as well as server principals of Hadoop/ZK which expects the host name to be populated
 * in specific way (localhost vs 127.0.0.1). This provides an abstraction to handle more than one Login Module
 * since the default {@link JaasConfiguration} behavior only supports global/unique principal identifier
 */

@Internal
public class TestingJaasConfiguration extends JaasConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(TestingJaasConfiguration.class);

	public Map<String, TestingSecurityContext.ClientSecurityConfiguration> clientSecurityConfigurationMap;

	TestingJaasConfiguration(String keytab, String principal, Map<String,
			TestingSecurityContext.ClientSecurityConfiguration> clientSecurityConfigurationMap) {
		super(keytab, principal);
		this.clientSecurityConfigurationMap = clientSecurityConfigurationMap;
	}

	@Override
	public AppConfigurationEntry[] getAppConfigurationEntry(String applicationName) {

		LOG.debug("In TestingJaasConfiguration - Application Requested: {}", applicationName);

		AppConfigurationEntry[] appConfigurationEntry = super.getAppConfigurationEntry(applicationName);

		if(clientSecurityConfigurationMap != null && clientSecurityConfigurationMap.size() > 0) {

			if(clientSecurityConfigurationMap.containsKey(applicationName)) {

				LOG.debug("In TestingJaasConfiguration - Application: {} found in the supplied context", applicationName);

				TestingSecurityContext.ClientSecurityConfiguration conf = clientSecurityConfigurationMap.get(applicationName);

				if(appConfigurationEntry != null && appConfigurationEntry.length > 0) {

					for(int count=0; count < appConfigurationEntry.length; count++) {

						AppConfigurationEntry ace = appConfigurationEntry[count];

						if (ace.getOptions().containsKey("keyTab")) {

							String keyTab = conf.getKeytab();
							String principal = conf.getPrincipal();

							LOG.debug("In TestingJaasConfiguration - Application: {} from the supplied context will " +
									"use Client Specific Keytab: {} and Principal: {}", applicationName, keyTab, principal);

							Map<String, String> newKeytabKerberosOptions = new HashMap<>();
							newKeytabKerberosOptions.putAll(getKeytabKerberosOptions());

							newKeytabKerberosOptions.put("keyTab", keyTab);
							newKeytabKerberosOptions.put("principal", principal);

							AppConfigurationEntry keytabKerberosAce = new AppConfigurationEntry(
									KerberosUtil.getKrb5LoginModuleName(),
									AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
									newKeytabKerberosOptions);
							appConfigurationEntry = new AppConfigurationEntry[] {keytabKerberosAce};

							LOG.debug("---->Login Module is using Keytab based configuration<------");
							LOG.debug("Login Module Name: " + keytabKerberosAce.getLoginModuleName());
							LOG.debug("Control Flag: " + keytabKerberosAce.getControlFlag());
							LOG.debug("Options: " + keytabKerberosAce.getOptions());
						}
					}
				}
			}

		}

		return appConfigurationEntry;
	}

}
