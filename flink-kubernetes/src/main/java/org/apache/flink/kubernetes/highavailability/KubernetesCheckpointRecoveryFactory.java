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

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;

import java.util.concurrent.Executor;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Factory to create {@link CompletedCheckpointStore} and {@link CheckpointIDCounter}. */
public class KubernetesCheckpointRecoveryFactory implements CheckpointRecoveryFactory {

    private final FlinkKubeClient kubeClient;

    private final Executor executor;

    // Function to get the ConfigMap name for checkpoint. Input is job id, and output is ConfigMap
    // name.
    private final Function<JobID, String> getConfigMapNameFunction;

    private final Configuration configuration;

    private final String lockIdentity;

    /**
     * Create a KubernetesCheckpointRecoveryFactory.
     *
     * @param kubeClient Kubernetes client
     * @param configuration Flink configuration
     * @param executor IO executor to run blocking calls
     * @param function Function to get the ConfigMap name for checkpoint.
     * @param lockIdentity Lock identity of current HA service
     */
    public KubernetesCheckpointRecoveryFactory(
            FlinkKubeClient kubeClient,
            Configuration configuration,
            Executor executor,
            Function<JobID, String> function,
            String lockIdentity) {

        this.kubeClient = checkNotNull(kubeClient);
        this.configuration = checkNotNull(configuration);
        this.executor = checkNotNull(executor);
        this.getConfigMapNameFunction = checkNotNull(function);
        this.lockIdentity = checkNotNull(lockIdentity);
    }

    @Override
    public CompletedCheckpointStore createRecoveredCompletedCheckpointStore(
            JobID jobID, int maxNumberOfCheckpointsToRetain, ClassLoader userClassLoader)
            throws Exception {

        final String configMapName = getConfigMapNameFunction.apply(jobID);
        return KubernetesUtils.createCompletedCheckpointStore(
                configuration,
                kubeClient,
                executor,
                configMapName,
                lockIdentity,
                maxNumberOfCheckpointsToRetain);
    }

    @Override
    public CheckpointIDCounter createCheckpointIDCounter(JobID jobID) {
        return new KubernetesCheckpointIDCounter(
                kubeClient, getConfigMapNameFunction.apply(jobID), lockIdentity);
    }
}
