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
import org.apache.flink.client.deployment.executors.RemoteExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link ClusterClientFactory} for a standalone cluster, i.e. Flink on bare-metal. */
@Internal
public class StandaloneClientFactory implements ClusterClientFactory<StandaloneClusterId> {

    @Override
    public boolean isCompatibleWith(Configuration configuration) {
        checkNotNull(configuration);
        return RemoteExecutor.NAME.equalsIgnoreCase(
                configuration.getString(DeploymentOptions.TARGET));
    }

    @Override
    public StandaloneClusterDescriptor createClusterDescriptor(Configuration configuration) {
        checkNotNull(configuration);
        return new StandaloneClusterDescriptor(configuration);
    }

    @Override
    @Nullable
    public StandaloneClusterId getClusterId(Configuration configuration) {
        checkNotNull(configuration);
        return StandaloneClusterId.getInstance();
    }

    @Override
    public ClusterSpecification getClusterSpecification(Configuration configuration) {
        checkNotNull(configuration);
        return new ClusterSpecification.ClusterSpecificationBuilder().createClusterSpecification();
    }
}
