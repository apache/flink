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

package org.apache.flink.kubernetes.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;

import javax.annotation.Nonnull;

/** An {@link PipelineExecutorFactory} for executing jobs on an existing (session) cluster. */
@Internal
public class KubernetesSessionClusterExecutorFactory implements PipelineExecutorFactory {

    @Override
    public String getName() {
        return KubernetesSessionClusterExecutor.NAME;
    }

    @Override
    public boolean isCompatibleWith(@Nonnull final Configuration configuration) {
        return configuration
                .get(DeploymentOptions.TARGET)
                .equalsIgnoreCase(KubernetesSessionClusterExecutor.NAME);
    }

    @Override
    public PipelineExecutor getExecutor(@Nonnull final Configuration configuration) {
        return new KubernetesSessionClusterExecutor(configuration);
    }
}
