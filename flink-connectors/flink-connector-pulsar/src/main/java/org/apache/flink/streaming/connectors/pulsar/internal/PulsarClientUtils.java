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

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.annotation.Internal;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;

import java.util.Properties;

/**
 * Utility to create Pulsar Admin Client from adminUrl and clientConfigurationData.
 */
@Internal
public class PulsarClientUtils {

	public static PulsarAdmin newAdminFromConf(
		String adminUrl,
		ClientConfigurationData clientConfigurationData) throws PulsarClientException {
		ClientConfigurationData adminConf = clientConfigurationData.clone();
		adminConf.setServiceUrl(adminUrl);
		setAuth(adminConf);
		return new PulsarAdmin(adminUrl, adminConf);
	}

	private static void setAuth(ClientConfigurationData conf) throws PulsarClientException {
		if (!StringUtils.isBlank(conf.getAuthPluginClassName())
			&& !StringUtils.isBlank(conf.getAuthParams())) {
			conf.setAuthentication(AuthenticationFactory.create(
				conf.getAuthPluginClassName(),
				conf.getAuthParams()));
		}
	}

	public static ClientConfigurationData newClientConf(String serviceUrl, Properties properties) {
		ClientConfigurationData clientConf = new ClientConfigurationData();
		clientConf.setServiceUrl(serviceUrl);
		if (properties != null) {
			clientConf.setAuthParams(properties.getProperty(PulsarOptions.AUTH_PARAMS_KEY));
			clientConf.setAuthPluginClassName(properties.getProperty(PulsarOptions.AUTH_PLUGIN_CLASSNAME_KEY));
		}
		return clientConf;
	}

}
