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

package org.apache.flink.kubernetes.kubeclient.factory;

import org.apache.flink.kubernetes.KubernetesClusterClientFactory;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerSpecification;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;

/** General tests for the {@link KubernetesJobManagerFactory} with pod template. */
public class KubernetesJobManagerFactoryWithPodTemplateTest
        extends KubernetesFactoryWithPodTemplateTestBase {

    @Override
    protected Pod getResultPod(FlinkPod podTemplate) throws Exception {
        final KubernetesJobManagerParameters kubernetesJobManagerParameters =
                new KubernetesJobManagerParameters(
                        flinkConfig,
                        new KubernetesClusterClientFactory().getClusterSpecification(flinkConfig));

        final KubernetesJobManagerSpecification kubernetesJobManagerSpecification =
                KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                        podTemplate, kubernetesJobManagerParameters);

        final PodTemplateSpec podTemplateSpec =
                kubernetesJobManagerSpecification.getDeployment().getSpec().getTemplate();

        return new PodBuilder()
                .withMetadata(podTemplateSpec.getMetadata())
                .withSpec(podTemplateSpec.getSpec())
                .build();
    }
}
