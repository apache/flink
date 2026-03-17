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

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** General tests for the {@link PersistentVolumeClaimMountDecorator}. */
class PersistentVolumeClaimMountDecoratorTest extends KubernetesJobManagerTestBase {

    private static final String PVC_NAME_1 = "checkpoint-pvc";
    private static final String PVC_MOUNT_PATH_1 = "/opt/flink/checkpoints";
    private static final String PVC_NAME_2 = "data-pvc";
    private static final String PVC_MOUNT_PATH_2 = "/opt/flink/data";

    private PersistentVolumeClaimMountDecorator pvcDecorator;

    @Override
    protected void setupFlinkConfig() {
        super.setupFlinkConfig();
        // Configure single PVC by default
        this.flinkConfig.setString(
                KubernetesConfigOptions.KUBERNETES_PERSISTENT_VOLUME_CLAIMS.key(),
                PVC_NAME_1 + ":" + PVC_MOUNT_PATH_1);
    }

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();
        this.pvcDecorator = new PersistentVolumeClaimMountDecorator(kubernetesJobManagerParameters);
    }

    @Test
    void testNoPvcConfigured() {
        // Reset config to have no PVC
        this.flinkConfig.removeConfig(KubernetesConfigOptions.KUBERNETES_PERSISTENT_VOLUME_CLAIMS);
        this.pvcDecorator = new PersistentVolumeClaimMountDecorator(kubernetesJobManagerParameters);

        final FlinkPod resultFlinkPod = pvcDecorator.decorateFlinkPod(baseFlinkPod);

        // Should return the same pod without modifications
        assertThat(resultFlinkPod.getPodWithoutMainContainer().getSpec().getVolumes())
                .isEqualTo(baseFlinkPod.getPodWithoutMainContainer().getSpec().getVolumes());
        assertThat(resultFlinkPod.getMainContainer().getVolumeMounts())
                .isEqualTo(baseFlinkPod.getMainContainer().getVolumeMounts());
    }

    @Test
    void testSinglePvcMount() {
        final FlinkPod resultFlinkPod = pvcDecorator.decorateFlinkPod(baseFlinkPod);

        final String expectedVolumeName =
                PVC_NAME_1 + PersistentVolumeClaimMountDecorator.VOLUME_NAME_SUFFIX;

        // Verify volume is added to pod
        assertThat(
                        VolumeTestUtils.podHasVolume(
                                resultFlinkPod.getPodWithoutMainContainer(), expectedVolumeName))
                .isTrue();

        // Verify volume mount is added to container
        assertThat(
                        VolumeTestUtils.containerHasVolume(
                                resultFlinkPod.getMainContainer(),
                                expectedVolumeName,
                                PVC_MOUNT_PATH_1))
                .isTrue();

        // Verify PVC claim name in volume
        final Volume volume =
                resultFlinkPod.getPodWithoutMainContainer().getSpec().getVolumes().stream()
                        .filter(v -> v.getName().equals(expectedVolumeName))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("Volume not found"));
        assertThat(volume.getPersistentVolumeClaim().getClaimName()).isEqualTo(PVC_NAME_1);
    }

    @Test
    void testMultiplePvcMount() {
        // Configure multiple PVCs
        this.flinkConfig.setString(
                KubernetesConfigOptions.KUBERNETES_PERSISTENT_VOLUME_CLAIMS.key(),
                PVC_NAME_1 + ":" + PVC_MOUNT_PATH_1 + "," + PVC_NAME_2 + ":" + PVC_MOUNT_PATH_2);
        this.pvcDecorator = new PersistentVolumeClaimMountDecorator(kubernetesJobManagerParameters);

        final FlinkPod resultFlinkPod = pvcDecorator.decorateFlinkPod(baseFlinkPod);

        final String expectedVolumeName1 =
                PVC_NAME_1 + PersistentVolumeClaimMountDecorator.VOLUME_NAME_SUFFIX;
        final String expectedVolumeName2 =
                PVC_NAME_2 + PersistentVolumeClaimMountDecorator.VOLUME_NAME_SUFFIX;

        // Verify both volumes are added
        assertThat(
                        VolumeTestUtils.podHasVolume(
                                resultFlinkPod.getPodWithoutMainContainer(), expectedVolumeName1))
                .isTrue();
        assertThat(
                        VolumeTestUtils.podHasVolume(
                                resultFlinkPod.getPodWithoutMainContainer(), expectedVolumeName2))
                .isTrue();

        // Verify both volume mounts are added
        assertThat(
                        VolumeTestUtils.containerHasVolume(
                                resultFlinkPod.getMainContainer(),
                                expectedVolumeName1,
                                PVC_MOUNT_PATH_1))
                .isTrue();
        assertThat(
                        VolumeTestUtils.containerHasVolume(
                                resultFlinkPod.getMainContainer(),
                                expectedVolumeName2,
                                PVC_MOUNT_PATH_2))
                .isTrue();
    }

    @Test
    void testReadOnlyMount() {
        // Configure PVC with read-only flag
        this.flinkConfig.set(
                KubernetesConfigOptions.KUBERNETES_PERSISTENT_VOLUME_CLAIM_READ_ONLY, true);
        this.pvcDecorator = new PersistentVolumeClaimMountDecorator(kubernetesJobManagerParameters);

        final FlinkPod resultFlinkPod = pvcDecorator.decorateFlinkPod(baseFlinkPod);

        final String expectedVolumeName =
                PVC_NAME_1 + PersistentVolumeClaimMountDecorator.VOLUME_NAME_SUFFIX;

        // Verify volume mount is read-only
        final VolumeMount volumeMount =
                resultFlinkPod.getMainContainer().getVolumeMounts().stream()
                        .filter(vm -> vm.getName().equals(expectedVolumeName))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("VolumeMount not found"));
        assertThat(volumeMount.getReadOnly()).isTrue();

        // Verify PVC volume source is read-only
        final Volume volume =
                resultFlinkPod.getPodWithoutMainContainer().getSpec().getVolumes().stream()
                        .filter(v -> v.getName().equals(expectedVolumeName))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("Volume not found"));
        assertThat(volume.getPersistentVolumeClaim().getReadOnly()).isTrue();
    }

    @Test
    void testReadWriteMount() {
        // Ensure read-only is false (default)
        this.flinkConfig.set(
                KubernetesConfigOptions.KUBERNETES_PERSISTENT_VOLUME_CLAIM_READ_ONLY, false);
        this.pvcDecorator = new PersistentVolumeClaimMountDecorator(kubernetesJobManagerParameters);

        final FlinkPod resultFlinkPod = pvcDecorator.decorateFlinkPod(baseFlinkPod);

        final String expectedVolumeName =
                PVC_NAME_1 + PersistentVolumeClaimMountDecorator.VOLUME_NAME_SUFFIX;

        // Verify volume mount is not read-only
        final VolumeMount volumeMount =
                resultFlinkPod.getMainContainer().getVolumeMounts().stream()
                        .filter(vm -> vm.getName().equals(expectedVolumeName))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("VolumeMount not found"));
        assertThat(volumeMount.getReadOnly()).isFalse();
    }

    // ==================== PVC Name Validation Tests ====================

    @Test
    void testValidPvcNamesAccepted() {
        // DNS-1123 subdomain allows '-' and '.'
        String[] validNames = {"my-pvc", "pvc1", "a", "my-long-pvc-name-123", "pvc.data-01"};
        for (String name : validNames) {
            this.flinkConfig.setString(
                    KubernetesConfigOptions.KUBERNETES_PERSISTENT_VOLUME_CLAIMS.key(),
                    name + ":/opt/flink/data");
            // Should not throw
            new PersistentVolumeClaimMountDecorator(kubernetesJobManagerParameters);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"My-PVC", "-my-pvc", "my-pvc-", "my_pvc", ".my-pvc", "my-pvc."})
    void testInvalidPvcNameFailsOnConstruction(String invalidPvcName) {
        this.flinkConfig.setString(
                KubernetesConfigOptions.KUBERNETES_PERSISTENT_VOLUME_CLAIMS.key(),
                invalidPvcName + ":/opt/flink");

        assertThatThrownBy(
                        () ->
                                new PersistentVolumeClaimMountDecorator(
                                        kubernetesJobManagerParameters))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DNS-1123 subdomain standard");
    }

    // ==================== Mount Path Validation Tests ====================

    @Test
    void testInvalidMountPathNotAbsoluteFailsOnConstruction() {
        this.flinkConfig.setString(
                KubernetesConfigOptions.KUBERNETES_PERSISTENT_VOLUME_CLAIMS.key(),
                "my-pvc:relative/path");

        assertThatThrownBy(
                        () ->
                                new PersistentVolumeClaimMountDecorator(
                                        kubernetesJobManagerParameters))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be an absolute path");
    }

    @Test
    void testDuplicateMountPathFailsOnConstruction() {
        this.flinkConfig.setString(
                KubernetesConfigOptions.KUBERNETES_PERSISTENT_VOLUME_CLAIMS.key(),
                "pvc1:/opt/flink,pvc2:/opt/flink");

        assertThatThrownBy(
                        () ->
                                new PersistentVolumeClaimMountDecorator(
                                        kubernetesJobManagerParameters))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate mount path");
    }

    // ==================== Volume Name Generation Tests ====================

    @Test
    void testVolumeNameSanitizationDotsReplacedWithHyphens() {
        // Configure PVC with dots (valid DNS-1123 subdomain)
        String pvcWithDots = "my.pvc.name";
        this.flinkConfig.setString(
                KubernetesConfigOptions.KUBERNETES_PERSISTENT_VOLUME_CLAIMS.key(),
                pvcWithDots + ":" + PVC_MOUNT_PATH_1);
        this.pvcDecorator = new PersistentVolumeClaimMountDecorator(kubernetesJobManagerParameters);

        final FlinkPod resultFlinkPod = pvcDecorator.decorateFlinkPod(baseFlinkPod);

        // Volume name should have dots replaced with hyphens + suffix
        String expectedVolumeName = "my-pvc-name-pvc";

        // Verify volume is added with sanitized name (no dots)
        assertThat(
                        VolumeTestUtils.podHasVolume(
                                resultFlinkPod.getPodWithoutMainContainer(), expectedVolumeName))
                .isTrue();

        // Verify volume name matches DNS-1123 label pattern
        assertThat(expectedVolumeName)
                .matches(PersistentVolumeClaimMountDecorator.DNS_1123_LABEL_PATTERN.pattern());
    }

    @Test
    void testVolumeNameExceedsMaxLengthFailsOnConstruction() {
        // PVC name that would result in volume name > 63 chars
        // Suffix is "-pvc" (4 chars), so PVC name > 59 chars will exceed 63 limit
        String longPvcName = "a".repeat(60);
        this.flinkConfig.setString(
                KubernetesConfigOptions.KUBERNETES_PERSISTENT_VOLUME_CLAIMS.key(),
                longPvcName + ":/opt/flink/data");

        assertThatThrownBy(
                        () ->
                                new PersistentVolumeClaimMountDecorator(
                                        kubernetesJobManagerParameters))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds the maximum length");
    }

    // ==================== Volume Conflict Detection Tests ====================

    @Test
    void testVolumeNameConflictFailFast() {
        // Create a FlinkPod with existing volume that has the same name as our PVC would generate
        String conflictingVolumeName =
                PVC_NAME_1 + PersistentVolumeClaimMountDecorator.VOLUME_NAME_SUFFIX;

        Volume existingVolume =
                new VolumeBuilder()
                        .withName(conflictingVolumeName)
                        .withNewEmptyDir()
                        .endEmptyDir()
                        .build();

        Pod podWithConflictingVolume =
                new PodBuilder(baseFlinkPod.getPodWithoutMainContainer())
                        .editOrNewSpec()
                        .addToVolumes(existingVolume)
                        .endSpec()
                        .build();

        FlinkPod flinkPodWithConflict =
                new FlinkPod.Builder()
                        .withPod(podWithConflictingVolume)
                        .withMainContainer(baseFlinkPod.getMainContainer())
                        .build();

        // Should throw IllegalArgumentException due to volume name conflict
        assertThatThrownBy(() -> pvcDecorator.decorateFlinkPod(flinkPodWithConflict))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Volume name conflict")
                .hasMessageContaining(conflictingVolumeName);
    }

    @Test
    void testPvcNameWithDotsConflictDetection() {
        // Configure PVC with dots
        this.flinkConfig.setString(
                KubernetesConfigOptions.KUBERNETES_PERSISTENT_VOLUME_CLAIMS.key(),
                "my.pvc.name:/opt/flink/data");
        this.pvcDecorator = new PersistentVolumeClaimMountDecorator(kubernetesJobManagerParameters);

        // Create a FlinkPod with existing volume that matches sanitized name
        String sanitizedVolumeName = "my-pvc-name-pvc"; // dots replaced with hyphens + suffix

        Volume existingVolume =
                new VolumeBuilder()
                        .withName(sanitizedVolumeName)
                        .withNewEmptyDir()
                        .endEmptyDir()
                        .build();

        Pod podWithConflictingVolume =
                new PodBuilder(baseFlinkPod.getPodWithoutMainContainer())
                        .editOrNewSpec()
                        .addToVolumes(existingVolume)
                        .endSpec()
                        .build();

        FlinkPod flinkPodWithConflict =
                new FlinkPod.Builder()
                        .withPod(podWithConflictingVolume)
                        .withMainContainer(baseFlinkPod.getMainContainer())
                        .build();

        // Should throw due to conflict with sanitized volume name
        assertThatThrownBy(() -> pvcDecorator.decorateFlinkPod(flinkPodWithConflict))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Volume name conflict");
    }

    @Test
    void testPvcWithDotsValidAndMountsCorrectly() {
        // Configure PVC with dots (valid DNS-1123 subdomain)
        String pvcWithDots = "my.namespace.pvc-data";
        this.flinkConfig.setString(
                KubernetesConfigOptions.KUBERNETES_PERSISTENT_VOLUME_CLAIMS.key(),
                pvcWithDots + ":" + PVC_MOUNT_PATH_1);
        this.pvcDecorator = new PersistentVolumeClaimMountDecorator(kubernetesJobManagerParameters);

        final FlinkPod resultFlinkPod = pvcDecorator.decorateFlinkPod(baseFlinkPod);

        // Volume name should have dots replaced with hyphens
        String expectedVolumeName = "my-namespace-pvc-data-pvc";

        // Verify volume is added with sanitized name
        assertThat(
                        VolumeTestUtils.podHasVolume(
                                resultFlinkPod.getPodWithoutMainContainer(), expectedVolumeName))
                .isTrue();

        // Verify PVC claim name is preserved (original with dots)
        final Volume volume =
                resultFlinkPod.getPodWithoutMainContainer().getSpec().getVolumes().stream()
                        .filter(v -> v.getName().equals(expectedVolumeName))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("Volume not found"));
        assertThat(volume.getPersistentVolumeClaim().getClaimName()).isEqualTo(pvcWithDots);
    }
}
