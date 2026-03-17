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

import org.apache.flink.annotation.Internal;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Mount user-specified PersistentVolumeClaims (PVCs) into JobManager and TaskManager pods.
 *
 * @see KubernetesConfigOptions#KUBERNETES_PERSISTENT_VOLUME_CLAIMS
 * @see KubernetesConfigOptions#KUBERNETES_PERSISTENT_VOLUME_CLAIM_READ_ONLY
 */
@Internal
public class PersistentVolumeClaimMountDecorator extends AbstractKubernetesStepDecorator {

    private static final Logger LOG =
            LoggerFactory.getLogger(PersistentVolumeClaimMountDecorator.class);

    /** Suffix appended to sanitized PVC names to generate volume names. */
    public static final String VOLUME_NAME_SUFFIX = "-pvc";

    /**
     * Maximum length for Kubernetes volume names (DNS-1123 label standard). Must be 63 characters
     * or less.
     */
    public static final int MAX_VOLUME_NAME_LENGTH = 63;

    /**
     * Maximum length for Kubernetes resource names (DNS-1123 subdomain standard). Must be 253
     * characters or less.
     */
    public static final int MAX_PVC_NAME_LENGTH = 253;

    /**
     * Pattern for validating DNS-1123 subdomain names. Used for PVC names. Must consist of lower
     * case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric
     * character.
     */
    public static final Pattern DNS_1123_SUBDOMAIN_PATTERN =
            Pattern.compile("^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$");

    /**
     * Pattern for validating DNS-1123 label names. Used for volume names. Must consist of lower
     * case alphanumeric characters or '-', and must start and end with an alphanumeric character.
     */
    public static final Pattern DNS_1123_LABEL_PATTERN =
            Pattern.compile("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$");

    private final Map<String, String> pvcNamesToMountPaths;
    private final boolean readOnly;

    public PersistentVolumeClaimMountDecorator(
            AbstractKubernetesParameters kubernetesComponentConf) {
        checkNotNull(kubernetesComponentConf, "kubernetesComponentConf must not be null");
        this.pvcNamesToMountPaths = kubernetesComponentConf.getPersistentVolumeClaimsToMountPaths();
        this.readOnly = kubernetesComponentConf.isPersistentVolumeClaimReadOnly();

        // Validate PVC configuration
        validatePvcConfiguration(pvcNamesToMountPaths);
    }

    private static void validatePvcConfiguration(Map<String, String> pvcNamesToMountPaths) {
        if (pvcNamesToMountPaths.isEmpty()) {
            return;
        }

        Set<String> mountPaths = new HashSet<>();
        Set<String> volumeNames = new HashSet<>();

        for (Map.Entry<String, String> entry : pvcNamesToMountPaths.entrySet()) {
            String pvcName = entry.getKey();
            String mountPath = entry.getValue();

            // Validate PVC name (DNS-1123 subdomain)
            validatePvcName(pvcName);

            // Validate mount path
            validateMountPath(mountPath, mountPaths);
            mountPaths.add(mountPath);

            // Check for volume name conflicts (after sanitization)
            String volumeName = getVolumeName(pvcName);
            checkArgument(
                    !volumeNames.contains(volumeName),
                    "Volume name conflict detected: PVC '%s' generates volume name '%s' which conflicts with another PVC. "
                            + "Please use more distinct PVC names.",
                    pvcName,
                    volumeName);
            volumeNames.add(volumeName);
        }
    }

    private static void validatePvcName(String pvcName) {
        checkArgument(
                pvcName != null && !pvcName.trim().isEmpty(),
                "PVC name must not be null or empty.");

        checkArgument(
                pvcName.length() <= MAX_PVC_NAME_LENGTH,
                "PVC name '%s' exceeds maximum length of %d characters.",
                pvcName,
                MAX_PVC_NAME_LENGTH);

        checkArgument(
                DNS_1123_SUBDOMAIN_PATTERN.matcher(pvcName).matches(),
                "PVC name '%s' is invalid. Must conform to DNS-1123 subdomain standard: "
                        + "lowercase alphanumeric characters, '-' or '.', must start and end with alphanumeric.",
                pvcName);
    }

    private static void validateMountPath(String mountPath, Set<String> existingPaths) {
        checkArgument(
                mountPath != null && !mountPath.trim().isEmpty(),
                "Mount path must not be null or empty.");

        checkArgument(
                mountPath.startsWith("/"),
                "Mount path '%s' must be an absolute path starting with '/'.",
                mountPath);

        checkArgument(
                !existingPaths.contains(mountPath),
                "Duplicate mount path detected: '%s'. Each PVC must have a unique mount path.",
                mountPath);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        if (pvcNamesToMountPaths.isEmpty()) {
            return flinkPod;
        }

        // Check for conflicts with existing volumes (fail-fast for volume name conflicts)
        checkVolumeConflicts(flinkPod);

        final Pod podWithVolumes = decoratePod(flinkPod.getPodWithoutMainContainer());
        final Container containerWithMounts = decorateMainContainer(flinkPod.getMainContainer());

        return new FlinkPod.Builder(flinkPod)
                .withPod(podWithVolumes)
                .withMainContainer(containerWithMounts)
                .build();
    }

    private void checkVolumeConflicts(FlinkPod flinkPod) {
        // Get existing volume names from pod spec
        Set<String> existingVolumeNames = new HashSet<>();
        if (flinkPod.getPodWithoutMainContainer().getSpec() != null
                && flinkPod.getPodWithoutMainContainer().getSpec().getVolumes() != null) {
            flinkPod.getPodWithoutMainContainer().getSpec().getVolumes().stream()
                    .map(Volume::getName)
                    .forEach(existingVolumeNames::add);
        }

        // Get existing mount paths from container
        Set<String> existingMountPaths = new HashSet<>();
        if (flinkPod.getMainContainer().getVolumeMounts() != null) {
            flinkPod.getMainContainer().getVolumeMounts().stream()
                    .map(VolumeMount::getMountPath)
                    .forEach(existingMountPaths::add);
        }

        // Check for conflicts
        for (Map.Entry<String, String> entry : pvcNamesToMountPaths.entrySet()) {
            String volumeName = getVolumeName(entry.getKey());
            String mountPath = entry.getValue();

            // Volume name conflict: fail-fast (K8s does not allow duplicate volume names)
            checkArgument(
                    !existingVolumeNames.contains(volumeName),
                    "Volume name conflict: PVC '%s' generates volume name '%s' which already exists in the Pod spec. "
                            + "This conflict may be caused by Pod Template configuration. "
                            + "Please use a different PVC name or remove the conflicting volume from Pod Template.",
                    entry.getKey(),
                    volumeName);

            // Mount path conflict: warn only (K8s allows but behavior may be unexpected)
            if (existingMountPaths.contains(mountPath)) {
                LOG.warn(
                        "Mount path '{}' for PVC '{}' conflicts with an existing mount in the Pod spec. "
                                + "The PVC mount will be added alongside the existing mount. "
                                + "Consider using unique mount paths to avoid unexpected behavior.",
                        mountPath,
                        entry.getKey());
            }
        }
    }

    private Container decorateMainContainer(Container container) {
        final List<VolumeMount> volumeMounts = buildVolumeMounts();
        return new ContainerBuilder(container).addAllToVolumeMounts(volumeMounts).build();
    }

    private Pod decoratePod(Pod pod) {
        final List<Volume> volumes = buildVolumes();
        return new PodBuilder(pod).editOrNewSpec().addAllToVolumes(volumes).endSpec().build();
    }

    private List<VolumeMount> buildVolumeMounts() {
        return pvcNamesToMountPaths.entrySet().stream()
                .map(this::buildVolumeMount)
                .collect(Collectors.toList());
    }

    private VolumeMount buildVolumeMount(Map.Entry<String, String> pvcNameToMountPath) {
        return new VolumeMountBuilder()
                .withName(getVolumeName(pvcNameToMountPath.getKey()))
                .withMountPath(pvcNameToMountPath.getValue())
                .withReadOnly(readOnly)
                .build();
    }

    private List<Volume> buildVolumes() {
        return pvcNamesToMountPaths.keySet().stream()
                .map(this::buildVolume)
                .collect(Collectors.toList());
    }

    private Volume buildVolume(String pvcName) {
        return new VolumeBuilder()
                .withName(getVolumeName(pvcName))
                .withPersistentVolumeClaim(
                        new PersistentVolumeClaimVolumeSourceBuilder()
                                .withClaimName(pvcName)
                                .withReadOnly(readOnly)
                                .build())
                .build();
    }

    /**
     * Generates a DNS-1123 label compliant volume name from the PVC name by replacing '.' with '-'
     * and appending '-pvc' suffix.
     */
    private static String getVolumeName(String pvcName) {
        // Sanitize: replace '.' with '-' to conform to DNS-1123 label
        String sanitized = pvcName.replace('.', '-');

        // Add suffix
        String volumeName = sanitized + VOLUME_NAME_SUFFIX;

        checkArgument(
                volumeName.length() <= MAX_VOLUME_NAME_LENGTH,
                "Generated volume name '%s' for PVC '%s' exceeds the maximum length of %d characters. "
                        + "Please use a shorter PVC name.",
                volumeName,
                pvcName,
                MAX_VOLUME_NAME_LENGTH);

        return volumeName;
    }
}
