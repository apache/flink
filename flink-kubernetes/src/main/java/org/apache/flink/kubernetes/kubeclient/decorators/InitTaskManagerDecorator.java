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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesToleration;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.NodeAffinity;
import io.fabric8.kubernetes.api.model.NodeAffinityBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.utils.Constants.API_VERSION;
import static org.apache.flink.kubernetes.utils.Constants.ENV_FLINK_POD_NODE_ID;
import static org.apache.flink.kubernetes.utils.Constants.POD_NODE_ID_FIELD_PATH;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** An initializer for the TaskManager {@link org.apache.flink.kubernetes.kubeclient.FlinkPod}. */
public class InitTaskManagerDecorator extends AbstractKubernetesStepDecorator {

    private final KubernetesTaskManagerParameters kubernetesTaskManagerParameters;
    private final Configuration flinkConfig;

    public InitTaskManagerDecorator(
            KubernetesTaskManagerParameters kubernetesTaskManagerParameters) {
        this.kubernetesTaskManagerParameters = checkNotNull(kubernetesTaskManagerParameters);
        this.flinkConfig = checkNotNull(kubernetesTaskManagerParameters.getFlinkConfiguration());
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        final PodBuilder basicPodBuilder = new PodBuilder(flinkPod.getPodWithoutMainContainer());

        // Overwrite fields
        final String serviceAccountName =
                KubernetesUtils.resolveUserDefinedValue(
                        flinkConfig,
                        KubernetesConfigOptions.TASK_MANAGER_SERVICE_ACCOUNT,
                        kubernetesTaskManagerParameters.getServiceAccount(),
                        KubernetesUtils.getServiceAccount(flinkPod),
                        "service account");

        final String dnsPolicy =
                KubernetesUtils.resolveDNSPolicy(
                        flinkPod.getPodWithoutMainContainer().getSpec().getDnsPolicy(),
                        kubernetesTaskManagerParameters.isHostNetworkEnabled());

        if (flinkPod.getPodWithoutMainContainer().getSpec().getRestartPolicy() != null) {
            logger.info(
                    "The restart policy of TaskManager pod will be overwritten to 'never' "
                            + "since it should not be restarted.");
        }
        basicPodBuilder
                .withApiVersion(Constants.API_VERSION)
                .editOrNewMetadata()
                .withName(kubernetesTaskManagerParameters.getPodName())
                .endMetadata()
                .editOrNewSpec()
                .withServiceAccount(serviceAccountName)
                .withServiceAccountName(serviceAccountName)
                .withRestartPolicy(Constants.RESTART_POLICY_OF_NEVER)
                .withHostNetwork(kubernetesTaskManagerParameters.isHostNetworkEnabled())
                .withDnsPolicy(dnsPolicy)
                .endSpec();

        // Merge fields
        basicPodBuilder
                .editOrNewMetadata()
                .addToLabels(kubernetesTaskManagerParameters.getLabels())
                .addToAnnotations(kubernetesTaskManagerParameters.getAnnotations())
                .endMetadata()
                .editOrNewSpec()
                .addToImagePullSecrets(kubernetesTaskManagerParameters.getImagePullSecrets())
                .addToNodeSelector(kubernetesTaskManagerParameters.getNodeSelector())
                .addAllToTolerations(
                        kubernetesTaskManagerParameters.getTolerations().stream()
                                .map(e -> KubernetesToleration.fromMap(e).getInternalResource())
                                .collect(Collectors.toList()))
                .endSpec();

        // Add node affinity.
        // https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#node-affinity
        Set<String> blockedNodes = kubernetesTaskManagerParameters.getBlockedNodes();
        if (!blockedNodes.isEmpty()) {
            basicPodBuilder
                    .editOrNewSpec()
                    .editOrNewAffinity()
                    .withNodeAffinity(
                            generateNodeAffinity(
                                    kubernetesTaskManagerParameters.getNodeNameLabel(),
                                    blockedNodes))
                    .endAffinity()
                    .endSpec();
        }

        final Container basicMainContainer = decorateMainContainer(flinkPod.getMainContainer());

        return new FlinkPod.Builder(flinkPod)
                .withPod(basicPodBuilder.build())
                .withMainContainer(basicMainContainer)
                .build();
    }

    private NodeAffinity generateNodeAffinity(String labelKey, Set<String> blockedNodes) {
        NodeSelectorRequirement nodeSelectorRequirement =
                new NodeSelectorRequirement(labelKey, "NotIn", new ArrayList<>(blockedNodes));

        NodeAffinityBuilder nodeAffinityBuilder = new NodeAffinityBuilder();
        return nodeAffinityBuilder
                .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                .addNewNodeSelectorTerm()
                .addToMatchExpressions(nodeSelectorRequirement)
                .endNodeSelectorTerm()
                .endRequiredDuringSchedulingIgnoredDuringExecution()
                .build();
    }

    private Container decorateMainContainer(Container container) {
        final ContainerBuilder mainContainerBuilder = new ContainerBuilder(container);

        // Overwrite fields
        final ResourceRequirements requirementsInPodTemplate =
                container.getResources() == null
                        ? new ResourceRequirements()
                        : container.getResources();
        final ResourceRequirements resourceRequirements =
                KubernetesUtils.getResourceRequirements(
                        requirementsInPodTemplate,
                        kubernetesTaskManagerParameters.getTaskManagerMemoryMB(),
                        kubernetesTaskManagerParameters.getTaskManagerMemoryLimitFactor(),
                        kubernetesTaskManagerParameters.getTaskManagerCPU(),
                        kubernetesTaskManagerParameters.getTaskManagerCPULimitFactor(),
                        kubernetesTaskManagerParameters.getTaskManagerExternalResources(),
                        kubernetesTaskManagerParameters.getTaskManagerExternalResourceConfigKeys());
        final String image =
                KubernetesUtils.resolveUserDefinedValue(
                        flinkConfig,
                        KubernetesConfigOptions.CONTAINER_IMAGE,
                        kubernetesTaskManagerParameters.getImage(),
                        container.getImage(),
                        "main container image");
        final String imagePullPolicy =
                KubernetesUtils.resolveUserDefinedValue(
                        flinkConfig,
                        KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY,
                        kubernetesTaskManagerParameters.getImagePullPolicy().name(),
                        container.getImagePullPolicy(),
                        "main container image pull policy");
        mainContainerBuilder
                .withName(Constants.MAIN_CONTAINER_NAME)
                .withImage(image)
                .withImagePullPolicy(imagePullPolicy)
                .withResources(resourceRequirements);

        // Merge fields
        mainContainerBuilder
                .addAllToPorts(getContainerPorts())
                .addAllToEnv(getCustomizedEnvs())
                .addNewEnv()
                .withName(ENV_FLINK_POD_NODE_ID)
                .withValueFrom(
                        new EnvVarSourceBuilder()
                                .withNewFieldRef(API_VERSION, POD_NODE_ID_FIELD_PATH)
                                .build())
                .endEnv();
        getFlinkLogDirEnv().ifPresent(mainContainerBuilder::addToEnv);

        return mainContainerBuilder.build();
    }

    private List<ContainerPort> getContainerPorts() {
        if (kubernetesTaskManagerParameters.isHostNetworkEnabled()) {
            return Collections.emptyList();
        }
        return Collections.singletonList(
                new ContainerPortBuilder()
                        .withName(Constants.TASK_MANAGER_RPC_PORT_NAME)
                        .withContainerPort(kubernetesTaskManagerParameters.getRPCPort())
                        .build());
    }

    private List<EnvVar> getCustomizedEnvs() {
        return kubernetesTaskManagerParameters.getEnvironments().entrySet().stream()
                .map(kv -> new EnvVar(kv.getKey(), kv.getValue(), null))
                .collect(Collectors.toList());
    }

    private Optional<EnvVar> getFlinkLogDirEnv() {
        return kubernetesTaskManagerParameters
                .getFlinkLogDirInPod()
                .map(logDir -> new EnvVar(Constants.ENV_FLINK_LOG_DIR, logDir, null));
    }
}
