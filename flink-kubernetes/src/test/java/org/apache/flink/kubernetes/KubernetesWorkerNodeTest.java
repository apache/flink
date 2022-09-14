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

package org.apache.flink.kubernetes;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KubernetesWorkerNode}. */
class KubernetesWorkerNodeTest {
    @Test
    public void testGetAttemptFromWorkerNode() throws Exception {
        final String correctPodName = "flink-on-kubernetes-taskmanager-11-5";
        final KubernetesWorkerNode workerNode =
                new KubernetesWorkerNode(new ResourceID(correctPodName));
        assertThat(workerNode.getAttempt()).isEqualTo(11L);

        final String wrongPodName = "flink-on-kubernetes-xxxtaskmanager-11-5";
        final KubernetesWorkerNode workerNode1 =
                new KubernetesWorkerNode(new ResourceID(wrongPodName));
        assertThatThrownBy(() -> workerNode1.getAttempt())
                .isInstanceOf(Exception.class)
                .hasMessageContaining(
                        "Error to parse KubernetesWorkerNode from " + wrongPodName + ".");
    }
}
