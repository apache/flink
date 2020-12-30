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

package org.apache.flink.kubernetes.kubeclient.resources;

import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.Pod;

import java.util.stream.Collectors;

/** Represent KubernetesPod resource in kubernetes. */
public class KubernetesPod extends KubernetesResource<Pod> {

    public KubernetesPod(Pod pod) {
        super(pod);
    }

    public String getName() {
        return this.getInternalResource().getMetadata().getName();
    }

    public boolean isTerminated() {
        if (getInternalResource().getStatus() != null) {
            return getInternalResource().getStatus().getContainerStatuses().stream()
                    .anyMatch(e -> e.getState() != null && e.getState().getTerminated() != null);
        }
        return false;
    }

    public String getTerminatedDiagnostics() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Pod terminated, container termination statuses: [");
        if (getInternalResource().getStatus() != null) {
            sb.append(
                    getInternalResource().getStatus().getContainerStatuses().stream()
                            .filter(
                                    containerStatus ->
                                            containerStatus.getState() != null
                                                    && containerStatus.getState().getTerminated()
                                                            != null)
                            .map(
                                    (containerStatus) -> {
                                        final ContainerStateTerminated containerStateTerminated =
                                                containerStatus.getState().getTerminated();
                                        return String.format(
                                                "%s(exitCode=%d, reason=%s, message=%s)",
                                                containerStatus.getName(),
                                                containerStateTerminated.getExitCode(),
                                                containerStateTerminated.getReason(),
                                                containerStateTerminated.getMessage());
                                    })
                            .collect(Collectors.joining(",")));
        }
        sb.append("]");
        return sb.toString();
    }
}
