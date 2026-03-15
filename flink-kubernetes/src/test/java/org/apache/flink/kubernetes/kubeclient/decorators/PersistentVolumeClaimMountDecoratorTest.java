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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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

    @Test
    void testVolumeNameGeneration() {
        final String pvcName = "my-test-pvc";
        final String expectedVolumeName =
                pvcName + PersistentVolumeClaimMountDecorator.VOLUME_NAME_SUFFIX;

        assertThat(PersistentVolumeClaimMountDecorator.getVolumeName(pvcName))
                .isEqualTo(expectedVolumeName);
    }

    @Test
    void testBuildVolumeMounts() {
        final List<VolumeMount> volumeMounts = pvcDecorator.buildVolumeMounts();

        assertThat(volumeMounts).hasSize(1);
        assertThat(volumeMounts.get(0).getName())
                .isEqualTo(PVC_NAME_1 + PersistentVolumeClaimMountDecorator.VOLUME_NAME_SUFFIX);
        assertThat(volumeMounts.get(0).getMountPath()).isEqualTo(PVC_MOUNT_PATH_1);
    }

    @Test
    void testBuildVolumes() {
        final List<Volume> volumes = pvcDecorator.buildVolumes();

        assertThat(volumes).hasSize(1);
        assertThat(volumes.get(0).getName())
                .isEqualTo(PVC_NAME_1 + PersistentVolumeClaimMountDecorator.VOLUME_NAME_SUFFIX);
        assertThat(volumes.get(0).getPersistentVolumeClaim().getClaimName()).isEqualTo(PVC_NAME_1);
    }

    // ==================== PVC Name Validation Tests (DNS-1123 Subdomain) ====================

    @Test
    void testValidatePvcName_ValidNames() {
        // These should not throw - DNS-1123 subdomain allows '-' and '.'
        PersistentVolumeClaimMountDecorator.validatePvcName("my-pvc");
        PersistentVolumeClaimMountDecorator.validatePvcName("pvc1");
        PersistentVolumeClaimMountDecorator.validatePvcName("a");
        PersistentVolumeClaimMountDecorator.validatePvcName("my-long-pvc-name-123");
        PersistentVolumeClaimMountDecorator.validatePvcName("a1b2c3");
        // DNS-1123 subdomain allows dots
        PersistentVolumeClaimMountDecorator.validatePvcName("pvc.data-01");
        PersistentVolumeClaimMountDecorator.validatePvcName("my.pvc.name");
        // Long names up to 253 chars are allowed for subdomain
        PersistentVolumeClaimMountDecorator.validatePvcName("a".repeat(253));
    }

    @Test
    void testValidatePvcName_NullOrEmpty() {
        assertThatThrownBy(() -> PersistentVolumeClaimMountDecorator.validatePvcName(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be null or empty");

        assertThatThrownBy(() -> PersistentVolumeClaimMountDecorator.validatePvcName(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be null or empty");

        assertThatThrownBy(() -> PersistentVolumeClaimMountDecorator.validatePvcName("   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be null or empty");
    }

    @Test
    void testValidatePvcName_InvalidDns1123Subdomain() {
        // Uppercase letters
        assertThatThrownBy(() -> PersistentVolumeClaimMountDecorator.validatePvcName("My-PVC"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DNS-1123 subdomain standard");

        // Starts with hyphen
        assertThatThrownBy(() -> PersistentVolumeClaimMountDecorator.validatePvcName("-my-pvc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DNS-1123 subdomain standard");

        // Ends with hyphen
        assertThatThrownBy(() -> PersistentVolumeClaimMountDecorator.validatePvcName("my-pvc-"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DNS-1123 subdomain standard");

        // Contains underscore
        assertThatThrownBy(() -> PersistentVolumeClaimMountDecorator.validatePvcName("my_pvc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DNS-1123 subdomain standard");

        // Starts with dot
        assertThatThrownBy(() -> PersistentVolumeClaimMountDecorator.validatePvcName(".my-pvc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DNS-1123 subdomain standard");

        // Ends with dot
        assertThatThrownBy(() -> PersistentVolumeClaimMountDecorator.validatePvcName("my-pvc."))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DNS-1123 subdomain standard");
    }

    @Test
    void testValidatePvcName_ExceedsMaxLength() {
        String longName = "a".repeat(254); // 254 chars, exceeds 253 limit
        assertThatThrownBy(() -> PersistentVolumeClaimMountDecorator.validatePvcName(longName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds maximum length");
    }

    // ==================== Mount Path Validation Tests ====================

    @Test
    void testValidateMountPath_ValidPaths() {
        HashSet<String> existingPaths = new HashSet<>();
        // These should not throw
        PersistentVolumeClaimMountDecorator.validateMountPath("/opt/flink", existingPaths);
        PersistentVolumeClaimMountDecorator.validateMountPath("/", new HashSet<>());
        PersistentVolumeClaimMountDecorator.validateMountPath("/a/b/c/d", new HashSet<>());
    }

    @Test
    void testValidateMountPath_NullOrEmpty() {
        assertThatThrownBy(
                        () ->
                                PersistentVolumeClaimMountDecorator.validateMountPath(
                                        null, new HashSet<>()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be null or empty");

        assertThatThrownBy(
                        () ->
                                PersistentVolumeClaimMountDecorator.validateMountPath(
                                        "", new HashSet<>()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be null or empty");
    }

    @Test
    void testValidateMountPath_NotAbsolute() {
        assertThatThrownBy(
                        () ->
                                PersistentVolumeClaimMountDecorator.validateMountPath(
                                        "relative/path", new HashSet<>()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be an absolute path");
    }

    @Test
    void testValidateMountPath_Duplicate() {
        HashSet<String> existingPaths = new HashSet<>();
        existingPaths.add("/opt/flink");

        assertThatThrownBy(
                        () ->
                                PersistentVolumeClaimMountDecorator.validateMountPath(
                                        "/opt/flink", existingPaths))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate mount path");
    }

    // ==================== Volume Name Generation Tests ====================

    @Test
    void testVolumeNameSanitization_DotsReplacedWithHyphens() {
        // PVC name with dots should have them replaced with hyphens in volume name
        String pvcWithDots = "my.pvc.name";
        String volumeName = PersistentVolumeClaimMountDecorator.getVolumeName(pvcWithDots);

        // Dots should be replaced with hyphens
        assertThat(volumeName).doesNotContain(".");
        assertThat(volumeName).isEqualTo("my-pvc-name-pvc");
    }

    @Test
    void testVolumeNameTruncationWithHash() {
        // PVC name that would result in volume name > 63 chars
        String longPvcName = "a".repeat(100);

        String volumeName = PersistentVolumeClaimMountDecorator.getVolumeName(longPvcName);

        // Should be truncated to max 63 chars
        assertThat(volumeName.length())
                .isLessThanOrEqualTo(PersistentVolumeClaimMountDecorator.MAX_VOLUME_NAME_LENGTH);

        // Should contain hash suffix for uniqueness
        assertThat(volumeName).endsWith("-pvc");

        // Should not end with hyphen (before suffix)
        assertThat(volumeName).doesNotEndWith("--pvc");
    }

    @Test
    void testVolumeNameUniquenessWithHash() {
        // Two long PVC names that would be truncated to the same prefix without hash
        String pvc1 = "a".repeat(60) + "x";
        String pvc2 = "a".repeat(60) + "y";

        String vol1 = PersistentVolumeClaimMountDecorator.getVolumeName(pvc1);
        String vol2 = PersistentVolumeClaimMountDecorator.getVolumeName(pvc2);

        // Volume names should be different due to hash
        assertThat(vol1).isNotEqualTo(vol2);

        // Both should be valid length
        assertThat(vol1.length())
                .isLessThanOrEqualTo(PersistentVolumeClaimMountDecorator.MAX_VOLUME_NAME_LENGTH);
        assertThat(vol2.length())
                .isLessThanOrEqualTo(PersistentVolumeClaimMountDecorator.MAX_VOLUME_NAME_LENGTH);
    }

    @Test
    void testVolumeNameValidDns1123Label() {
        // Even with dots in PVC name, volume name should be valid DNS-1123 label
        String pvcWithDots = "my.namespace.pvc-01";
        String volumeName = PersistentVolumeClaimMountDecorator.getVolumeName(pvcWithDots);

        // Should match DNS-1123 label pattern (no dots)
        assertThat(volumeName)
                .matches(PersistentVolumeClaimMountDecorator.DNS_1123_LABEL_PATTERN.pattern());
    }

    // ==================== Volume Conflict Detection Tests (Fail-Fast) ====================

    @Test
    void testVolumeNameConflict_FailFast() {
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

    // ==================== Configuration Validation Tests ====================

    @Test
    void testValidatePvcConfiguration_ValidConfig() {
        Map<String, String> validPvcs = new HashMap<>();
        validPvcs.put("pvc1", "/path1");
        validPvcs.put("pvc2", "/path2");
        validPvcs.put("pvc.with.dots", "/path3");

        // Should not throw
        PersistentVolumeClaimMountDecorator.validatePvcConfiguration(validPvcs);
    }

    @Test
    void testValidatePvcConfiguration_EmptyMap() {
        // Should not throw
        PersistentVolumeClaimMountDecorator.validatePvcConfiguration(Collections.emptyMap());
    }

    @Test
    void testInvalidPvcName_FailsOnConstruction() {
        // Configure an invalid PVC name
        this.flinkConfig.setString(
                KubernetesConfigOptions.KUBERNETES_PERSISTENT_VOLUME_CLAIMS.key(),
                "Invalid_PVC_Name:/opt/flink");

        assertThatThrownBy(
                        () ->
                                new PersistentVolumeClaimMountDecorator(
                                        kubernetesJobManagerParameters))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DNS-1123 subdomain standard");
    }

    @Test
    void testInvalidMountPath_FailsOnConstruction() {
        // Configure a relative mount path
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
    void testDuplicateMountPath_FailsOnConstruction() {
        // Configure duplicate mount paths
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
