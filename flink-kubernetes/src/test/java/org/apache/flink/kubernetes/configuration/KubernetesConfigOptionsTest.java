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

package org.apache.flink.kubernetes.configuration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.FallbackKey;

import org.junit.jupiter.api.Test;

import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KubernetesConfigOptions}. */
class KubernetesConfigOptionsTest {

    private static final String DEPRECATED_KEY = "kubernetes.pod-template-file";
    private static final String DEFAULT_KEY = "kubernetes.pod-template-file.default";

    @Test
    void testPodTemplateDefaultKeyTakesPrecedenceOverDeprecatedKey() {
        final Configuration configuration = new Configuration();
        configuration.setString(DEPRECATED_KEY, "/deprecated.yaml");
        configuration.setString(DEFAULT_KEY, "/default.yaml");

        assertThat(configuration.getOptional(KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE))
                .hasValue("/default.yaml");
        assertThat(configuration.getOptional(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE))
                .hasValue("/default.yaml");
        assertThat(configuration.getOptional(KubernetesConfigOptions.KUBERNETES_POD_TEMPLATE))
                .hasValue("/default.yaml");
    }

    @Test
    void testPodTemplateDeprecatedKeyIsStillHonoredOnItsOwn() {
        final Configuration configuration = new Configuration();
        configuration.setString(DEPRECATED_KEY, "/deprecated.yaml");

        assertThat(configuration.getOptional(KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE))
                .hasValue("/deprecated.yaml");
        assertThat(configuration.getOptional(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE))
                .hasValue("/deprecated.yaml");
    }

    @Test
    void testPodTemplateLegacyKeyIsRegisteredAsDeprecated() {
        assertThat(isDeprecatedKey(KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE))
                .as(
                        "%s must be deprecated so that using it logs a deprecation warning",
                        DEPRECATED_KEY)
                .isTrue();
        assertThat(isDeprecatedKey(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE))
                .as(
                        "%s must be deprecated so that using it logs a deprecation warning",
                        DEPRECATED_KEY)
                .isTrue();
    }

    private static boolean isDeprecatedKey(ConfigOption<?> option) {
        return StreamSupport.stream(option.fallbackKeys().spliterator(), false)
                .filter(fallbackKey -> DEPRECATED_KEY.equals(fallbackKey.getKey()))
                .anyMatch(FallbackKey::isDeprecated);
    }
}
