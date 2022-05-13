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

import org.apache.flink.kubernetes.KubernetesPodTemplateTestUtils;
import org.apache.flink.kubernetes.KubernetesTestBase;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkPod}. */
class FlinkPodTest extends KubernetesTestBase {

    @Test
    void testCopyFlinkPod() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        final FlinkPod copiedFlinkPod = flinkPod.copy();
        assertThat(flinkPod).isNotSameAs(copiedFlinkPod);
        assertThat(flinkPod.getPodWithoutMainContainer())
                .isNotSameAs(copiedFlinkPod.getPodWithoutMainContainer());
        assertThat(flinkPod.getPodWithoutMainContainer())
                .isEqualTo(copiedFlinkPod.getPodWithoutMainContainer());
        assertThat(flinkPod.getMainContainer()).isNotSameAs(copiedFlinkPod.getMainContainer());
        assertThat(flinkPod.getMainContainer()).isEqualTo(copiedFlinkPod.getMainContainer());
    }
}
