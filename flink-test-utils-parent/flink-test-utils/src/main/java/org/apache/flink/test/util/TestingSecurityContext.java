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
import org.apache.flink.runtime.security.DynamicConfiguration;
import org.apache.flink.runtime.security.KerberosUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.security.modules.JaasModuleFactory;
import org.apache.flink.runtime.security.modules.SecurityModuleFactory;

import javax.security.auth.login.AppConfigurationEntry;

import java.util.Map;

/**
 * Test security context to support handling both client and server principals in MiniKDC.
 * This class is used only in integration test code for connectors like Kafka, HDFS etc.,
 */
@Internal
public class TestingSecurityContext {

	public static void install(SecurityConfiguration config,
						Map<String, ClientSecurityConfiguration> clientSecurityConfigurationMap)
			throws Exception {

		SecurityUtils.install(config);

		// install dynamic JAAS entries
		for (SecurityModuleFactory factory : config.getSecurityModuleFactories()) {
			if (factory instanceof JaasModuleFactory) {
				DynamicConfiguration jaasConf = (DynamicConfiguration) javax.security.auth.login.Configuration.getConfiguration();
				for (Map.Entry<String, ClientSecurityConfiguration> e : clientSecurityConfigurationMap.entrySet()) {
					AppConfigurationEntry entry = KerberosUtils.keytabEntry(
						e.getValue().getKeytab(),
						e.getValue().getPrincipal());
					jaasConf.addAppConfigurationEntry(e.getKey(), entry);
				}
				break;
			}
		}
	}

	static class ClientSecurityConfiguration {

		private final String principal;

		private final String keytab;

		public String getPrincipal() {
			return principal;
		}

		public String getKeytab() {
			return keytab;
		}

		public ClientSecurityConfiguration(String principal, String keytab) {
			this.principal = principal;
			this.keytab = keytab;
		}
	}

}
