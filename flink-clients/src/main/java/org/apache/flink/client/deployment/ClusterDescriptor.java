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


import org.apache.flink.client.program.ClusterClient;

/**
 * A descriptor to deploy a cluster (e.g. Yarn or Mesos) and return a Client for Cluster communication.
 */
public interface ClusterDescriptor<ClientType extends ClusterClient> {

	/**
	 * Returns a String containing details about the cluster (NodeManagers, available memory, ...)
	 *
	 */
	String getClusterDescription();

	/**
	 * Retrieves an existing Flink Cluster.
	 * @param applicationID The unique application identifier of the running cluster
	 * @return Client for the cluster
	 * @throws UnsupportedOperationException if this cluster descriptor doesn't support the operation
	 */
	ClientType retrieve(String applicationID) throws UnsupportedOperationException;

	/**
	 * Triggers deployment of a cluster
	 * @return Client for the cluster
	 * @throws UnsupportedOperationException if this cluster descriptor doesn't support the operation
	 */
	ClientType deploy() throws UnsupportedOperationException;
}
