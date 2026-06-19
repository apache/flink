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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link KubernetesDeploymentTarget}. */
class KubernetesDeploymentTargetTest {

    @Test
    void testCorrectInstantiationFromConfiguration() {
        for (KubernetesDeploymentTarget t : KubernetesDeploymentTarget.values()) {
            testCorrectInstantiationFromConfigurationHelper(t);
        }
    }

    @Test
    public void testInvalidInstantiationFromConfiguration() {
        final Configuration configuration = getConfigurationWithTarget("invalid-target");
        assertThatThrownBy(() -> KubernetesDeploymentTarget.fromConfig(configuration))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testNullInstantiationFromConfiguration() {
        assertThatThrownBy(() -> KubernetesDeploymentTarget.fromConfig(new Configuration()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testThatAValidOptionIsValid() {
        assertThat(
                        KubernetesDeploymentTarget.isValidKubernetesTarget(
                                KubernetesDeploymentTarget.APPLICATION.getName()))
                .isTrue();
    }

    @Test
    void testThatAnInvalidOptionIsInvalid() {
        assertThat(KubernetesDeploymentTarget.isValidKubernetesTarget("invalid-target")).isFalse();
    }

    private void testCorrectInstantiationFromConfigurationHelper(
            final KubernetesDeploymentTarget expectedDeploymentTarget) {
        final Configuration configuration =
                getConfigurationWithTarget(expectedDeploymentTarget.getName().toUpperCase());
        final KubernetesDeploymentTarget actualDeploymentTarget =
                KubernetesDeploymentTarget.fromConfig(configuration);

        assertThat(expectedDeploymentTarget).isSameAs(actualDeploymentTarget);
    }

    private Configuration getConfigurationWithTarget(final String target) {
        final Configuration configuration = new Configuration();
        configuration.set(DeploymentOptions.TARGET, target);
        return configuration;
    }
}
