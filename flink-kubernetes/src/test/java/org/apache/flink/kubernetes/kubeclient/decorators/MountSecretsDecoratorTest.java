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

import org.apache.flink.kubernetes.VolumeTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** General tests for the {@link MountSecretsDecorator}. */
public class MountSecretsDecoratorTest extends KubernetesJobManagerTestBase {

    private static final String SECRET_NAME = "test";
    private static final String SECRET_MOUNT_PATH = "/opt/flink/secret";

    private MountSecretsDecorator mountSecretsDecorator;

    @Override
    protected void setupFlinkConfig() {
        super.setupFlinkConfig();

        this.flinkConfig.setString(
                KubernetesConfigOptions.KUBERNETES_SECRETS.key(),
                SECRET_NAME + ":" + SECRET_MOUNT_PATH);
    }

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        this.mountSecretsDecorator = new MountSecretsDecorator(kubernetesJobManagerParameters);
    }

    @Test
    public void testWhetherPodOrContainerIsDecorated() {
        final FlinkPod resultFlinkPod = mountSecretsDecorator.decorateFlinkPod(baseFlinkPod);

        assertFalse(VolumeTestUtils.podHasVolume(baseFlinkPod.getPod(), SECRET_NAME + "-volume"));
        assertTrue(VolumeTestUtils.podHasVolume(resultFlinkPod.getPod(), SECRET_NAME + "-volume"));

        assertFalse(
                VolumeTestUtils.containerHasVolume(
                        baseFlinkPod.getMainContainer(),
                        SECRET_NAME + "-volume",
                        SECRET_MOUNT_PATH));
        assertTrue(
                VolumeTestUtils.containerHasVolume(
                        resultFlinkPod.getMainContainer(),
                        SECRET_NAME + "-volume",
                        SECRET_MOUNT_PATH));
    }
}
