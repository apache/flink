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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;

import javax.annotation.Nullable;

/**
 * A factory containing all the necessary information for creating clients to Flink clusters.
 */
@Internal
public interface ClusterClientFactory<ClusterID> {

	/**
	 * Returns {@code true} if the current {@link ClusterClientFactory} is compatible with the provided configuration,
	 * {@code false} otherwise.
	 */
	boolean isCompatibleWith(Configuration configuration);

	/**
	 * Create a {@link ClusterDescriptor} from the given configuration.
	 *
	 * @param configuration containing the configuration options relevant for the {@link ClusterDescriptor}
	 * @return the corresponding {@link ClusterDescriptor}.
	 */
	ClusterDescriptor<ClusterID> createClusterDescriptor(Configuration configuration);

	/**
	 * Returns the cluster id if a cluster id is specified in the provided configuration, otherwise it returns {@code null}.
	 *
	 * <p>A cluster id identifies a running cluster, e.g. the Yarn application id for a Flink cluster running on Yarn.
	 *
	 * @param configuration containing the configuration options relevant for the cluster id retrieval
	 * @return Cluster id identifying the cluster to deploy jobs to or null
	 */
	@Nullable
	ClusterID getClusterId(Configuration configuration);

	/**
	 * Returns the {@link ClusterSpecification} specified by the configuration and the command
	 * line options. This specification can be used to deploy a new Flink cluster.
	 *
	 * @param configuration containing the configuration options relevant for the {@link ClusterSpecification}
	 * @return the corresponding {@link ClusterSpecification} for a new Flink cluster
	 */
	ClusterSpecification getClusterSpecification(Configuration configuration);
}
