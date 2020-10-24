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

package org.apache.flink.yarn;

import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

/**
 * Factory interface for {@link AMRMClientAsync}.
 */
public interface YarnResourceManagerClientFactory {

	/**
	 * Create YARN ResourceManager client with the given callback handler.
	 *
	 * @param yarnHeartbeatIntervalMillis heartbeat interval between the client and the YARN ResourceManager.
	 * @param callbackHandler which handles the events from YARN ResourceManager.
	 * @return an {@link AMRMClientAsync} instance.
	 */
	AMRMClientAsync<AMRMClient.ContainerRequest> createResourceManagerClient(
		int yarnHeartbeatIntervalMillis,
		AMRMClientAsync.CallbackHandler callbackHandler);
}
