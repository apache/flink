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

package org.apache.flink.kubernetes.client;

/**
 * The client to talk with kubernetes.
 * */
public interface KubernetesClient extends AutoCloseable {

	/**
	 * Create kubernetes services and expose endpoints for access outside cluster.
	 */
	Endpoint createClusterService();

	/**
	 * Create cluster pod.
	 */
	void createClusterPod();

	/**
	 * stop cluster and clean up all resources, include services, auxiliary services and all running pods.
	 * */
	void stopAndCleanupCluster(String clusterId);

	/**
	 * Log exception.
	 * */
	void logException(Exception e);

	/**
	 * retrieval rest endpoint of the giving flink clusterId.
	 */
	Endpoint getResetEndpoint(String flinkClusterId);
}
