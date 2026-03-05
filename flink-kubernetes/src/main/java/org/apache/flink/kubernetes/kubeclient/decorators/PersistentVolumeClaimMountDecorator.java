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
import org.apache.flink.annotation.VisibleForTesting;
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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Decorator for mounting Kubernetes PersistentVolumeClaims (PVCs) to JobManager and TaskManager
 * pods.
 *
 * <p>This decorator allows users to attach pre-existing PVCs to Flink pods, enabling persistent
 * storage for checkpoints, savepoints, or other data that needs to survive pod restarts.
 *
 * <h2>Configuration</h2>
 *
 * <p>Users can configure PVC mounting through the following configuration options:
 *
 * <ul>
 *   <li>{@link KubernetesConfigOptions#KUBERNETES_PERSISTENT_VOLUME_CLAIMS} - Specifies PVCs and
 *       their mount paths in the format {@code pvc-name:/mount/path,pvc-name2:/mount/path2}
 *   <li>{@link KubernetesConfigOptions#KUBERNETES_PERSISTENT_VOLUME_CLAIM_READ_ONLY} - When set to
 *       true, mounts all PVCs as read-only (default: false)
 * </ul>
 *
 * <h2>Usage Example</h2>
 *
 * <pre>{@code
 * # Mount a single PVC for checkpoint storage
 * kubernetes.persistent-volume-claims: checkpoint-pvc:/opt/flink/checkpoints
 *
 * # Mount multiple PVCs
 * kubernetes.persistent-volume-claims: checkpoint-pvc:/opt/flink/checkpoints,data-pvc:/opt/flink/data
 *
 * # Mount PVCs as read-only
 * kubernetes.persistent-volume-claims.read-only: true
 * }</pre>
 *
 * <h2>Validation</h2>
 *
 * <p>This decorator validates that:
 *
 * <ul>
 *   <li>PVC names conform to DNS-1123 subdomain standard (lowercase alphanumeric, '-' and '.', max
 *       253 chars)
 *   <li>Mount paths are non-empty and absolute (start with '/')
 *   <li>No duplicate mount paths are specified
 *   <li>Generated volume names do not conflict with existing volumes
 * </ul>
 *
 * <h2>Volume Name Generation</h2>
 *
 * <p>Volume names are generated from PVC names by:
 *
 * <ol>
 *   <li>Replacing '.' with '-' (DNS-1123 label requirement)
 *   <li>Adding '-pvc' suffix
 *   <li>Truncating to 63 characters if necessary (with hash suffix for uniqueness)
 * </ol>
 *
 * <h2>Important Notes</h2>
 *
 * <ul>
 *   <li>The PVC must exist in the same namespace as the Flink cluster
 *   <li>The PVC must have an appropriate access mode (ReadWriteOnce, ReadWriteMany, etc.)
 *   <li>For HA setups with multiple JobManagers, use ReadWriteMany or ReadOnlyMany access modes
 *   <li>If you need different read/write modes for different PVCs, use Pod Templates instead
 * </ul>
 *
 * @see KubernetesConfigOptions#KUBERNETES_PERSISTENT_VOLUME_CLAIMS
 * @see KubernetesConfigOptions#KUBERNETES_PERSISTENT_VOLUME_CLAIM_READ_ONLY
 */
@Internal
public class PersistentVolumeClaimMountDecorator extends AbstractKubernetesStepDecorator {

    private static final Logger LOG =
            LoggerFactory.getLogger(PersistentVolumeClaimMountDecorator.class);

    /** Suffix appended to sanitized PVC names to generate volume names. */
    @VisibleForTesting static final String VOLUME_NAME_SUFFIX = "-pvc";

    /**
     * Maximum length for Kubernetes volume names (DNS-1123 label standard). Must be 63 characters
     * or less.
     */
    @VisibleForTesting static final int MAX_VOLUME_NAME_LENGTH = 63;

    /**
     * Maximum length for Kubernetes resource names (DNS-1123 subdomain standard). Must be 253
     * characters or less.
     */
    @VisibleForTesting static final int MAX_PVC_NAME_LENGTH = 253;

    /** Length reserved for hash suffix when truncating volume names. */
    private static final int HASH_SUFFIX_LENGTH = 6; // "-" + 5 hex chars

    /**
     * Pattern for validating DNS-1123 subdomain names. Used for PVC names. Must consist of lower
     * case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric
     * character.
     */
    @VisibleForTesting
    static final Pattern DNS_1123_SUBDOMAIN_PATTERN =
            Pattern.compile("^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$");

    /**
     * Pattern for validating DNS-1123 label names. Used for volume names. Must consist of lower
     * case alphanumeric characters or '-', and must start and end with an alphanumeric character.
     */
    @VisibleForTesting
    static final Pattern DNS_1123_LABEL_PATTERN =
            Pattern.compile("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$");

    private final Map<String, String> pvcNamesToMountPaths;
    private final boolean readOnly;

    /**
     * Creates a new PersistentVolumeClaimMountDecorator.
     *
     * @param kubernetesComponentConf the Kubernetes parameters containing PVC configuration
     * @throws IllegalArgumentException if the PVC configuration is invalid
     */
    public PersistentVolumeClaimMountDecorator(
            AbstractKubernetesParameters kubernetesComponentConf) {
        checkNotNull(kubernetesComponentConf, "kubernetesComponentConf must not be null");
        this.pvcNamesToMountPaths = kubernetesComponentConf.getPersistentVolumeClaimsToMountPaths();
        this.readOnly = kubernetesComponentConf.isPersistentVolumeClaimReadOnly();

        // Validate PVC configuration
        validatePvcConfiguration(pvcNamesToMountPaths);
    }

    /**
     * Validates the PVC configuration.
     *
     * @param pvcNamesToMountPaths the map of PVC names to mount paths
     * @throws IllegalArgumentException if validation fails
     */
    @VisibleForTesting
    static void validatePvcConfiguration(Map<String, String> pvcNamesToMountPaths) {
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

    /**
     * Validates that a PVC name conforms to DNS-1123 subdomain standard.
     *
     * <p>DNS-1123 subdomain allows:
     *
     * <ul>
     *   <li>Lowercase alphanumeric characters, '-' and '.'
     *   <li>Must start and end with an alphanumeric character
     *   <li>Maximum length of 253 characters
     * </ul>
     *
     * @param pvcName the PVC name to validate
     * @throws IllegalArgumentException if the PVC name is invalid
     */
    @VisibleForTesting
    static void validatePvcName(String pvcName) {
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

    /**
     * Validates the mount path.
     *
     * @param mountPath the mount path to validate
     * @param existingPaths existing mount paths to check for duplicates
     * @throws IllegalArgumentException if the mount path is invalid
     */
    @VisibleForTesting
    static void validateMountPath(String mountPath, Set<String> existingPaths) {
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

    /**
     * Checks for conflicts between PVC volumes and existing Pod volumes/mounts.
     *
     * <p>For volume name conflicts, this method throws an exception (fail-fast) because Kubernetes
     * does not allow duplicate volume names in a Pod spec. For mount path conflicts, this method
     * logs a warning since Kubernetes allows multiple volume mounts, though the behavior may be
     * unexpected.
     *
     * @param flinkPod the FlinkPod to check
     * @throws IllegalArgumentException if a volume name conflict is detected
     */
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

    /**
     * Decorates the main container with volume mounts for all configured PVCs.
     *
     * @param container the original main container
     * @return a new container with PVC volume mounts added
     */
    private Container decorateMainContainer(Container container) {
        final List<VolumeMount> volumeMounts = buildVolumeMounts();
        return new ContainerBuilder(container).addAllToVolumeMounts(volumeMounts).build();
    }

    /**
     * Decorates the pod specification with volumes for all configured PVCs.
     *
     * @param pod the original pod without main container
     * @return a new pod with PVC volumes added
     */
    private Pod decoratePod(Pod pod) {
        final List<Volume> volumes = buildVolumes();
        return new PodBuilder(pod).editOrNewSpec().addAllToVolumes(volumes).endSpec().build();
    }

    /**
     * Builds the list of volume mounts for all configured PVCs.
     *
     * @return list of volume mounts
     */
    @VisibleForTesting
    List<VolumeMount> buildVolumeMounts() {
        return pvcNamesToMountPaths.entrySet().stream()
                .map(this::buildVolumeMount)
                .collect(Collectors.toList());
    }

    /**
     * Builds a single volume mount for a PVC.
     *
     * @param pvcNameToMountPath entry containing PVC name and mount path
     * @return the volume mount
     */
    private VolumeMount buildVolumeMount(Map.Entry<String, String> pvcNameToMountPath) {
        return new VolumeMountBuilder()
                .withName(getVolumeName(pvcNameToMountPath.getKey()))
                .withMountPath(pvcNameToMountPath.getValue())
                .withReadOnly(readOnly)
                .build();
    }

    /**
     * Builds the list of volumes for all configured PVCs.
     *
     * @return list of volumes
     */
    @VisibleForTesting
    List<Volume> buildVolumes() {
        return pvcNamesToMountPaths.keySet().stream()
                .map(this::buildVolume)
                .collect(Collectors.toList());
    }

    /**
     * Builds a single volume for a PVC.
     *
     * @param pvcName the name of the PVC
     * @return the volume
     */
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
     * Generates a valid Kubernetes volume name from the PVC name.
     *
     * <p>Volume names must conform to DNS-1123 label standard:
     *
     * <ul>
     *   <li>Lowercase alphanumeric characters or '-'
     *   <li>Must start and end with an alphanumeric character
     *   <li>Maximum 63 characters
     * </ul>
     *
     * <p>The conversion process:
     *
     * <ol>
     *   <li>Replace '.' with '-' (dots are allowed in PVC names but not in volume names)
     *   <li>Append '-pvc' suffix
     *   <li>If length exceeds 63, truncate and append hash for uniqueness
     * </ol>
     *
     * @param pvcName the name of the PVC
     * @return a valid volume name
     */
    @VisibleForTesting
    static String getVolumeName(String pvcName) {
        // Sanitize: replace '.' with '-' to conform to DNS-1123 label
        String sanitized = pvcName.replace('.', '-');

        // Add suffix
        String volumeName = sanitized + VOLUME_NAME_SUFFIX;

        // Truncate if exceeds max length
        if (volumeName.length() > MAX_VOLUME_NAME_LENGTH) {
            volumeName = truncateWithHash(sanitized, pvcName);
        }

        // Final validation: ensure it doesn't end with '-'
        while (volumeName.endsWith("-")) {
            volumeName = volumeName.substring(0, volumeName.length() - 1);
        }

        return volumeName;
    }

    /**
     * Truncates the volume name and appends a hash suffix for uniqueness.
     *
     * <p>Format: {truncated-name}-{hash}-pvc
     *
     * @param sanitized the sanitized PVC name (with '.' replaced by '-')
     * @param original the original PVC name (used for hash calculation)
     * @return truncated volume name with hash suffix
     */
    private static String truncateWithHash(String sanitized, String original) {
        // Calculate hash from original PVC name for uniqueness
        String hash = String.format("%05x", original.hashCode() & 0xFFFFF);

        // Calculate available length for the base name
        // Format: {base}-{hash}-pvc, where hash is 5 chars
        int availableLength =
                MAX_VOLUME_NAME_LENGTH - HASH_SUFFIX_LENGTH - VOLUME_NAME_SUFFIX.length();

        // Truncate base name
        String truncatedBase =
                sanitized.substring(0, Math.min(sanitized.length(), availableLength));

        // Remove trailing hyphens from truncated base
        while (truncatedBase.endsWith("-")) {
            truncatedBase = truncatedBase.substring(0, truncatedBase.length() - 1);
        }

        return truncatedBase + "-" + hash.toLowerCase(Locale.ROOT) + VOLUME_NAME_SUFFIX;
    }
}
