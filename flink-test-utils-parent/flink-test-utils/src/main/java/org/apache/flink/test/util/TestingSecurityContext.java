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
import org.apache.flink.runtime.security.SecurityUtils;

import java.util.Map;

/*
 * Test security context to support handling both client and server principals in MiniKDC
 * This class is used only in integration test code for connectors like Kafka, HDFS etc.,
 */
@Internal
public class TestingSecurityContext {

	public static void install(SecurityUtils.SecurityConfiguration config,
						Map<String, ClientSecurityConfiguration> clientSecurityConfigurationMap)
			throws Exception {

		SecurityUtils.install(config);

		// establish the JAAS config for Test environment
		TestingJaasConfiguration jaasConfig = new TestingJaasConfiguration(config.getKeytab(),
				config.getPrincipal(), clientSecurityConfigurationMap);
		javax.security.auth.login.Configuration.setConfiguration(jaasConfig);
	}

	public static class ClientSecurityConfiguration {

		private String principal;

		private String keytab;

		private String moduleName;

		private String jaasServiceName;

		public String getPrincipal() {
			return principal;
		}

		public String getKeytab() {
			return keytab;
		}

		public String getModuleName() {
			return moduleName;
		}

		public String getJaasServiceName() {
			return jaasServiceName;
		}

		public ClientSecurityConfiguration(String principal, String keytab, String moduleName, String jaasServiceName) {
			this.principal = principal;
			this.keytab = keytab;
			this.moduleName = moduleName;
			this.jaasServiceName = jaasServiceName;
		}

	}

}
