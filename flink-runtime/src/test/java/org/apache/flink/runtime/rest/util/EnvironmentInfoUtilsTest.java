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

package org.apache.flink.runtime.rest.util;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link EnvironmentInfoUtils}. */
class EnvironmentInfoUtilsTest {

    @Test
    void readsContextFromCompleteEnvironment() {
        final Optional<EnvironmentInfoUtils.EnvironmentContext> context =
                EnvironmentInfoUtils.getEnvironmentContext(completeEnvironment());

        assertThat(context).isPresent();
        assertThat(context.get().containerId).isEqualTo("container_1");
        assertThat(context.get().nodeManagerHostName).isEqualTo("nm.example.com");
        assertThat(context.get().nodeManagerHttpPort).isEqualTo("8042");
        assertThat(context.get().user).isEqualTo("flink");
    }

    @Test
    void emptyIfAnyVariableIsMissingOrBlank() {
        for (String key : completeEnvironment().keySet()) {
            final Map<String, String> missing = completeEnvironment();
            missing.remove(key);
            assertThat(EnvironmentInfoUtils.getEnvironmentContext(missing)).isEmpty();

            final Map<String, String> blank = completeEnvironment();
            blank.put(key, " ");
            assertThat(EnvironmentInfoUtils.getEnvironmentContext(blank)).isEmpty();
        }
    }

    @Test
    void createsNodeManagerLogUrl() {
        assertThat(
                        EnvironmentInfoUtils.createNodeManagerLogUrl(
                                "nm.example.com", "8042", "container_1", "flink"))
                .isEqualTo("http://nm.example.com:8042/node/containerlogs/container_1/flink");
    }

    private static Map<String, String> completeEnvironment() {
        final Map<String, String> env = new HashMap<>();
        env.put("CONTAINER_ID", "container_1");
        env.put("NM_HOST", "nm.example.com");
        env.put("NM_HTTP_PORT", "8042");
        env.put("USER", "flink");
        return env;
    }
}
