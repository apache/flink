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

import org.apache.flink.runtime.security.SecurityUtils;

/**
 * Responsible for installing a process-wide ZooKeeper security configuration.
 */
public class ZooKeeperModule implements SecurityModule {

	private static final String ZOOKEEPER_SASL_CLIENT_USERNAME = "zookeeper.sasl.client.username";

	/**
	 * A system property for setting whether ZK uses SASL.
	 */
	private static final String ZK_ENABLE_CLIENT_SASL = "zookeeper.sasl.client";

	/**
	 * A system property for setting the expected ZooKeeper service name.
	 */
	private static final String ZK_SASL_CLIENT_USERNAME = "zookeeper.sasl.client.username";

	/**
	 * A system property for setting the login context name to use.
	 */
	private static final String ZK_LOGIN_CONTEXT_NAME = "zookeeper.sasl.clientconfig";

	private String priorSaslEnable;

	private String priorServiceName;

	private String priorLoginContextName;

	@Override
	public void install(SecurityUtils.SecurityConfiguration configuration) throws SecurityInstallException {

		priorSaslEnable = System.getProperty(ZK_ENABLE_CLIENT_SASL, null);
		System.setProperty(ZK_ENABLE_CLIENT_SASL, String.valueOf(!configuration.isZkSaslDisable()));

		priorServiceName = System.getProperty(ZK_SASL_CLIENT_USERNAME, null);
		if (!"zookeeper".equals(configuration.getZooKeeperServiceName())) {
			System.setProperty(ZK_SASL_CLIENT_USERNAME, configuration.getZooKeeperServiceName());
		}

		priorLoginContextName = System.getProperty(ZK_LOGIN_CONTEXT_NAME, null);
		if (!"Client".equals(configuration.getZooKeeperLoginContextName())) {
			System.setProperty(ZK_LOGIN_CONTEXT_NAME, configuration.getZooKeeperLoginContextName());
		}
	}

	@Override
	public void uninstall() throws SecurityInstallException {
		if(priorSaslEnable != null) {
			System.setProperty(ZK_ENABLE_CLIENT_SASL, priorSaslEnable);
		} else {
			System.clearProperty(ZK_ENABLE_CLIENT_SASL);
		}
		if(priorServiceName != null) {
			System.setProperty(ZK_SASL_CLIENT_USERNAME, priorServiceName);
		} else {
			System.clearProperty(ZK_SASL_CLIENT_USERNAME);
		}
		if(priorLoginContextName != null) {
			System.setProperty(ZK_LOGIN_CONTEXT_NAME, priorLoginContextName);
		} else {
			System.clearProperty(ZK_LOGIN_CONTEXT_NAME);
		}
	}

}
