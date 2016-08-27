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
package org.apache.flink.client.deployment;

import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;


/**
 * A deployment descriptor for an existing cluster
 */
public class StandaloneClusterDescriptor implements ClusterDescriptor<StandaloneClusterClient> {

	private final Configuration config;

	public StandaloneClusterDescriptor(Configuration config) {
		this.config = config;
	}

	@Override
	public String getClusterDescription() {
		String host = config.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "");
		int port = config.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, -1);
		return "Standalone cluster at " + host + ":" + port;
	}

	@Override
	public StandaloneClusterClient retrieve(String applicationID) {
		try {
			return new StandaloneClusterClient(config);
		} catch (Exception e) {
			throw new RuntimeException("Couldn't retrieve standalone cluster", e);
		}
	}

	@Override
	public StandaloneClusterClient deploy() throws UnsupportedOperationException {
		throw new UnsupportedOperationException("Can't deploy a standalone cluster.");
	}
}
