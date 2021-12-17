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

import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;

import io.fabric8.kubernetes.api.model.Pod;

import java.util.UUID;

/** General tests for the {@link KubernetesTaskManagerFactory} with pod template. */
public class KubernetesTaskManagerFactoryWithPodTemplateTest
        extends KubernetesFactoryWithPodTemplateTestBase {

    @Override
    protected Pod getResultPod(FlinkPod podTemplate) {
        return KubernetesTaskManagerFactory.buildTaskManagerKubernetesPod(
                        podTemplate,
                        KubernetesTestUtils.createTaskManagerParameters(
                                flinkConfig, "taskmanager-" + UUID.randomUUID().toString()))
                .getInternalResource();
    }
}
