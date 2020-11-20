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

import org.apache.flink.configuration.Configuration;

import java.util.stream.Stream;

/**
 * An interface used to discover the appropriate {@link ClusterClientFactory cluster client factory} based on the
 * provided {@link Configuration}.
 */
public interface ClusterClientServiceLoader {

	/**
	 * Discovers the appropriate {@link ClusterClientFactory} based on the provided configuration.
	 *
	 * @param configuration the configuration based on which the appropriate factory is going to be used.
	 * @return the appropriate {@link ClusterClientFactory}.
	 */
	<ClusterID> ClusterClientFactory<ClusterID> getClusterClientFactory(final Configuration configuration);

	/**
	 * Loads and returns a stream of the names of all available
	 * execution target names for {@code Application Mode}.
	 */
	Stream<String> getApplicationModeTargetNames();
}
