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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;

import io.fabric8.kubernetes.api.model.HasMetadata;

import java.util.List;

/** Composition of the created Kubernetes components that represents a Flink Task Manager. */
public class KubernetesTaskManagerSpecification {
    private KubernetesPod kubernetesPod;

    private List<HasMetadata> accompanyingResources;

    public KubernetesTaskManagerSpecification(
            KubernetesPod kubernetesPod, List<HasMetadata> accompanyingResources) {
        this.kubernetesPod = kubernetesPod;
        this.accompanyingResources = accompanyingResources;
    }

    public KubernetesPod getKubernetesPod() {
        return kubernetesPod;
    }

    public List<HasMetadata> getAccompanyingResources() {
        return accompanyingResources;
    }
}
