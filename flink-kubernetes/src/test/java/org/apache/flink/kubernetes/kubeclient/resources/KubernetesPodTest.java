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

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KubernetesPod}. */
class KubernetesPodTest {

    @Test
    void testIsTerminatedShouldReturnTrueWhenPodFailed() {
        final Pod pod = new PodBuilder().build();
        pod.setStatus(
                new PodStatusBuilder()
                        .withPhase(KubernetesPod.PodPhase.Failed.name())
                        .withMessage("Pod Node didn't have enough resource")
                        .withReason("OutOfMemory")
                        .build());
        assertThat(new KubernetesPod(pod).isTerminated()).isTrue();
    }
}
