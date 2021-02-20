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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.kubernetes.kubeclient.FlinkPod;

import io.fabric8.kubernetes.api.model.HasMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * An abstract {@link KubernetesStepDecorator} contains common implementations for different plug-in
 * features.
 */
public abstract class AbstractKubernetesStepDecorator implements KubernetesStepDecorator {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Apply transformations on the given FlinkPod in accordance to this feature. Note that we
     * should return a FlinkPod that keeps all of the properties of the passed FlinkPod object.
     *
     * <p>So this is correct:
     *
     * <pre>{@code
     * Pod decoratedPod = new PodBuilder(pod) // Keeps the original state
     *     ...
     *     .build()
     *
     * Container decoratedContainer = new ContainerBuilder(container) // Keeps the original state
     *     ...
     *     .build()
     *
     * FlinkPod decoratedFlinkPod = new FlinkPodBuilder(flinkPod) // Keeps the original state
     *     ...
     *     .build()
     *
     * }</pre>
     *
     * <p>And this is the incorrect:
     *
     * <pre>{@code
     * Pod decoratedPod = new PodBuilder() // Loses the original state
     *     ...
     *     .build()
     *
     * Container decoratedContainer = new ContainerBuilder() // Loses the original state
     *     ...
     *     .build()
     *
     * FlinkPod decoratedFlinkPod = new FlinkPodBuilder() // Loses the original state
     *     ...
     *     .build()
     *
     * }</pre>
     */
    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        return flinkPod;
    }

    /**
     * Note that the method could have a side effect of modifying the Flink Configuration object,
     * such as update the JobManager address.
     */
    @Override
    public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
        return Collections.emptyList();
    }
}
