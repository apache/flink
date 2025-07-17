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

package org.apache.flink.runtime.util;

import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ContextGetterTest {

    @Test
    void testGetContextWithContainerEnvironment() {
        Map<String, String> envVars = new HashMap<>(System.getenv());

        envVars.put("CONTAINER_ID", "container_e123_4567890123456789_01_000001");

        CommonTestUtils.setEnv(envVars, true);

        String result = ContextGetter.getContext("job123", "taskA");
        assertThat(result)
                .isEqualTo("FLINK_application_4567890123456789_01_JobID_job123_TaskName_taskA");
    }

    @Test
    void testGetContextWithKubernetesEnvironment() {
        Map<String, String> envVars = new HashMap<>(System.getenv());
        envVars.put("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc");
        envVars.put("HOSTNAME", "flink-taskmanager-pod-xyz");
        CommonTestUtils.setEnv(envVars, true);

        String result = ContextGetter.getContext("job456", "taskB");
        assertThat(result)
                .isEqualTo("FLINK_pod_flink-taskmanager-pod-xyz_JobID_job456_TaskName_taskB");
    }

    @Test
    void testGetContextInLocalEnvironment() {
        // Empty environment - no special vars set

        String result = ContextGetter.getContext("job789", "taskC");
        assertThat(result).isEqualTo("FLINK_local_JobID_job789_TaskName_taskC");
    }
}
