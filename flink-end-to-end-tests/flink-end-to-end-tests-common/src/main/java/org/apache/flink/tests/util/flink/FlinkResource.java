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

package org.apache.flink.tests.util.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.tests.util.util.FactoryUtils;
import org.apache.flink.util.ExternalResource;

import java.io.IOException;

/**
 * Generic interface for interacting with Flink.
 */
public interface FlinkResource extends ExternalResource {

	/**
	 * Adds the given configuration to the existing configuration of this resource. Entries in the existing configuration
	 * will be overwritten.
	 *
	 * @param config config to add
	 * @throws IOException
	 */
	void addConfiguration(Configuration config) throws IOException;

	/**
	 * Starts a cluster.
	 *
	 * <p>The exact constellation of the cluster is undefined.
	 *
	 * <p>In the case of per-job clusters this method may not start any Flink processes, deferring this to
	 * {@link ClusterController#submitJob(JobSubmission)}.
	 *
	 * @return controller for interacting with the cluster
	 * @throws IOException
	 * @param numTaskManagers number of task managers
	 */
	ClusterController startCluster(int numTaskManagers) throws IOException;

	/**
	 * Returns the configured FlinkResource implementation, or a {@link LocalStandaloneFlinkResource} if none is configured.
	 *
	 * @return configured FlinkResource, or {@link LocalStandaloneFlinkResource} is none is configured
	 */
	static FlinkResource get() {
		return FactoryUtils.loadAndInvokeFactory(
			FlinkResourceFactory.class,
			FlinkResourceFactory::create,
			LocalStandaloneFlinkResourceFactory::new);
	}
}
