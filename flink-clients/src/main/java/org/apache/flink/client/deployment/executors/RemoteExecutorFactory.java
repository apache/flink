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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;

/** An {@link PipelineExecutorFactory} for {@link RemoteExecutor remote executors}. */
@Internal
public class RemoteExecutorFactory implements PipelineExecutorFactory {

    @Override
    public String getName() {
        return RemoteExecutor.NAME;
    }

    @Override
    public boolean isCompatibleWith(final Configuration configuration) {
        return RemoteExecutor.NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET));
    }

    @Override
    public PipelineExecutor getExecutor(final Configuration configuration) {
        return new RemoteExecutor(configuration);
    }
}
