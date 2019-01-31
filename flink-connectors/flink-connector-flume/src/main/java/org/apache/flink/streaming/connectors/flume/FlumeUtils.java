/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.flume;

import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;

import java.util.Properties;

/**
 * Flume RpcClient Util.
 */
public class FlumeUtils {
	private static final String CLIENT_TYPE_KEY = "client.type";
	private static final String CLIENT_TYPE_DEFAULT_FAILOVER = "default_failover";
	private static final String CLIENT_TYPE_DEFAULT_LOADBALANCING = "default_loadbalance";
	private static final String DEFAULT_CLIENT_TYPE = "thrift";

	public static RpcClient getRpcClient(String clientType, String hostname, Integer port, Integer batchSize) {
		if (null == clientType) {
			clientType = DEFAULT_CLIENT_TYPE;
		}

		Properties props;
		RpcClient client;
		switch(clientType.toUpperCase()) {
			case "THRIFT":
				client = RpcClientFactory.getThriftInstance(hostname, port, batchSize);
				break;
			case "DEFAULT":
				client = RpcClientFactory.getDefaultInstance(hostname, port, batchSize);
				break;
			case "DEFAULT_FAILOVER":
				props = getDefaultProperties(hostname, port, batchSize);
				props.put(CLIENT_TYPE_KEY, CLIENT_TYPE_DEFAULT_FAILOVER);
				client = RpcClientFactory.getInstance(props);
				break;
			case "DEFAULT_LOADBALANCE":
				props = getDefaultProperties(hostname, port, batchSize);
				props.put(CLIENT_TYPE_KEY, CLIENT_TYPE_DEFAULT_LOADBALANCING);
				client = RpcClientFactory.getInstance(props);
				break;
			default:
				throw new IllegalStateException("Unsupported client type - cannot happen");
		}
		return client;
	}

	public static void destroy(RpcClient client) {
		if (null != client) {
			client.close();
		}
	}

	private static Properties getDefaultProperties(String hostname, Integer port, Integer batchSize) {
		Properties props = new Properties();
		props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1");
		props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + "h1",
			hostname + ":" + port.intValue());
		props.setProperty(RpcClientConfigurationConstants.CONFIG_BATCH_SIZE, batchSize.toString());
		return props;
	}
}
