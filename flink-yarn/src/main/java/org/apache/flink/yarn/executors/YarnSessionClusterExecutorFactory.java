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

package org.apache.flink.yarn.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.SessionClusterExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.Executor;
import org.apache.flink.core.execution.ExecutorFactory;
import org.apache.flink.yarn.YarnClusterClientFactory;

import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * An {@link ExecutorFactory} for executing jobs on an existing YARN session cluster.
 */
@Internal
public class YarnSessionClusterExecutorFactory implements ExecutorFactory {

	public static final String NAME = "yarn-session";

	@Override
	public boolean isCompatibleWith(final Configuration configuration) {
		return NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET));
	}

	@Override
	public Executor getExecutor(final Configuration configuration) {
		final YarnClusterClientFactory clientFactory = new YarnClusterClientFactory();
		try (final ClusterDescriptor<ApplicationId> descriptor = clientFactory.createClusterDescriptor(configuration)) {
			return new SessionClusterExecutor<>(descriptor.retrieve(clientFactory.getClusterId(configuration)));
		}
	}
}
