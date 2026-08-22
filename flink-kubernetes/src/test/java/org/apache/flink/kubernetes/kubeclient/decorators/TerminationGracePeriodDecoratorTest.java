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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesTaskManagerTestBase;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ExecActionBuilder;
import io.fabric8.kubernetes.api.model.PodBuilder;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link TerminationGracePeriodDecorator}. */
class TerminationGracePeriodDecoratorTest extends KubernetesTaskManagerTestBase {

    private static final Duration GRACE_PERIOD = Duration.ofSeconds(45);

    private TerminationGracePeriodDecorator decorator;

    @Override
    public void onSetup() throws Exception {
        super.onSetup();
        this.decorator =
                new TerminationGracePeriodDecorator(
                        kubernetesTaskManagerParameters,
                        KubernetesConfigOptions.TASK_MANAGER_TERMINATION_GRACE_PERIOD);
    }

    @Test
    void testUnsetOptionLeavesPodUnchanged() {
        final FlinkPod resultFlinkPod = decorator.decorateFlinkPod(baseFlinkPod);

        assertThat(resultFlinkPod.getPodWithoutMainContainer())
                .isEqualTo(baseFlinkPod.getPodWithoutMainContainer());
        assertThat(resultFlinkPod.getMainContainer()).isEqualTo(baseFlinkPod.getMainContainer());
    }

    @Test
    void testSetOptionAddsGracePeriodAndPreStopHook() {
        flinkConfig.set(
                KubernetesConfigOptions.TASK_MANAGER_TERMINATION_GRACE_PERIOD, GRACE_PERIOD);

        final FlinkPod resultFlinkPod = decorator.decorateFlinkPod(baseFlinkPod);

        assertThat(
                        resultFlinkPod
                                .getPodWithoutMainContainer()
                                .getSpec()
                                .getTerminationGracePeriodSeconds())
                .isEqualTo(GRACE_PERIOD.getSeconds());
        assertThat(resultFlinkPod.getMainContainer().getLifecycle())
                .isNotNull()
                .extracting(lifecycle -> lifecycle.getPreStop().getExec().getCommand())
                .isEqualTo(
                        java.util.Arrays.asList(
                                "sh", "-c", "kill -USR2 1 2>/dev/null || true; sleep 40"));
    }

    @Test
    void testSleepClampedToZeroForShortGracePeriods() {
        flinkConfig.set(
                KubernetesConfigOptions.TASK_MANAGER_TERMINATION_GRACE_PERIOD,
                Duration.ofSeconds(2));

        final FlinkPod resultFlinkPod = decorator.decorateFlinkPod(baseFlinkPod);

        assertThat(
                        resultFlinkPod
                                .getMainContainer()
                                .getLifecycle()
                                .getPreStop()
                                .getExec()
                                .getCommand())
                .isEqualTo(
                        java.util.Arrays.asList(
                                "sh", "-c", "kill -USR2 1 2>/dev/null || true; sleep 0"));
    }

    @Test
    void testDoesNotOverwriteExistingGracePeriod() {
        flinkConfig.set(
                KubernetesConfigOptions.TASK_MANAGER_TERMINATION_GRACE_PERIOD, GRACE_PERIOD);
        final FlinkPod podWithExistingGracePeriod =
                new FlinkPod.Builder(baseFlinkPod)
                        .withPod(
                                new PodBuilder(baseFlinkPod.getPodWithoutMainContainer())
                                        .editOrNewSpec()
                                        .withTerminationGracePeriodSeconds(999L)
                                        .endSpec()
                                        .build())
                        .build();

        final FlinkPod resultFlinkPod = decorator.decorateFlinkPod(podWithExistingGracePeriod);

        assertThat(
                        resultFlinkPod
                                .getPodWithoutMainContainer()
                                .getSpec()
                                .getTerminationGracePeriodSeconds())
                .isEqualTo(999L);
    }

    @Test
    void testDoesNotOverwriteExistingPreStopHook() {
        flinkConfig.set(
                KubernetesConfigOptions.TASK_MANAGER_TERMINATION_GRACE_PERIOD, GRACE_PERIOD);
        final FlinkPod podWithExistingPreStop =
                new FlinkPod.Builder(baseFlinkPod)
                        .withMainContainer(
                                new ContainerBuilder(baseFlinkPod.getMainContainer())
                                        .editOrNewLifecycle()
                                        .withNewPreStop()
                                        .withExec(
                                                new ExecActionBuilder().withCommand("true").build())
                                        .endPreStop()
                                        .endLifecycle()
                                        .build())
                        .build();

        final FlinkPod resultFlinkPod = decorator.decorateFlinkPod(podWithExistingPreStop);

        assertThat(
                        resultFlinkPod
                                .getMainContainer()
                                .getLifecycle()
                                .getPreStop()
                                .getExec()
                                .getCommand())
                .containsExactly("true");
    }
}
