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
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesToleration;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.utils.Constants.API_VERSION;
import static org.apache.flink.kubernetes.utils.Constants.ENV_FLINK_POD_IP_ADDRESS;
import static org.apache.flink.kubernetes.utils.Constants.POD_IP_FIELD_PATH;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** An initializer for the JobManager {@link org.apache.flink.kubernetes.kubeclient.FlinkPod}. */
public class InitJobManagerDecorator extends AbstractKubernetesStepDecorator {

    private final KubernetesJobManagerParameters kubernetesJobManagerParameters;
    private final Configuration flinkConfig;

    public InitJobManagerDecorator(KubernetesJobManagerParameters kubernetesJobManagerParameters) {
        this.kubernetesJobManagerParameters = checkNotNull(kubernetesJobManagerParameters);
        this.flinkConfig = checkNotNull(kubernetesJobManagerParameters.getFlinkConfiguration());
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        final PodBuilder basicPodBuilder = new PodBuilder(flinkPod.getPodWithoutMainContainer());

        // Overwrite fields
        final String serviceAccountName =
                KubernetesUtils.resolveUserDefinedValue(
                        flinkConfig,
                        KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT,
                        kubernetesJobManagerParameters.getServiceAccount(),
                        KubernetesUtils.getServiceAccount(flinkPod),
                        "service account");

        final String dnsPolicy =
                KubernetesUtils.resolveDNSPolicy(
                        flinkPod.getPodWithoutMainContainer().getSpec().getDnsPolicy(),
                        kubernetesJobManagerParameters.isHostNetworkEnabled());

        if (flinkPod.getPodWithoutMainContainer().getSpec().getRestartPolicy() != null) {
            logger.info(
                    "The restart policy of JobManager pod will be overwritten to 'always' "
                            + "since it is controlled by the Kubernetes deployment.");
        }

        basicPodBuilder
                .withApiVersion(API_VERSION)
                .editOrNewSpec()
                .withServiceAccount(serviceAccountName)
                .withServiceAccountName(serviceAccountName)
                .withHostNetwork(kubernetesJobManagerParameters.isHostNetworkEnabled())
                .withDnsPolicy(dnsPolicy)
                .endSpec();

        // Merge fields
        basicPodBuilder
                .editOrNewMetadata()
                .addToLabels(kubernetesJobManagerParameters.getLabels())
                .addToAnnotations(kubernetesJobManagerParameters.getAnnotations())
                .endMetadata()
                .editOrNewSpec()
                .addToImagePullSecrets(kubernetesJobManagerParameters.getImagePullSecrets())
                .addToNodeSelector(kubernetesJobManagerParameters.getNodeSelector())
                .addAllToTolerations(
                        kubernetesJobManagerParameters.getTolerations().stream()
                                .map(e -> KubernetesToleration.fromMap(e).getInternalResource())
                                .collect(Collectors.toList()))
                .endSpec();

        final Container basicMainContainer = decorateMainContainer(flinkPod.getMainContainer());

        return new FlinkPod.Builder(flinkPod)
                .withPod(basicPodBuilder.build())
                .withMainContainer(basicMainContainer)
                .build();
    }

    private Container decorateMainContainer(Container container) {
        final ContainerBuilder mainContainerBuilder = new ContainerBuilder(container);
        // Overwrite fields
        final String image =
                KubernetesUtils.resolveUserDefinedValue(
                        flinkConfig,
                        KubernetesConfigOptions.CONTAINER_IMAGE,
                        kubernetesJobManagerParameters.getImage(),
                        container.getImage(),
                        "main container image");
        final String imagePullPolicy =
                KubernetesUtils.resolveUserDefinedValue(
                        flinkConfig,
                        KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY,
                        kubernetesJobManagerParameters.getImagePullPolicy().name(),
                        container.getImagePullPolicy(),
                        "main container image pull policy");
        final ResourceRequirements requirementsInPodTemplate =
                container.getResources() == null
                        ? new ResourceRequirements()
                        : container.getResources();
        final ResourceRequirements requirements =
                KubernetesUtils.getResourceRequirements(
                        requirementsInPodTemplate,
                        kubernetesJobManagerParameters.getJobManagerMemoryMB(),
                        kubernetesJobManagerParameters.getJobManagerMemoryLimitFactor(),
                        kubernetesJobManagerParameters.getJobManagerCPU(),
                        kubernetesJobManagerParameters.getJobManagerCPULimitFactor(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        mainContainerBuilder
                .withName(Constants.MAIN_CONTAINER_NAME)
                .withImage(image)
                .withImagePullPolicy(imagePullPolicy)
                .withResources(requirements);

        // Merge fields
        mainContainerBuilder
                .addAllToPorts(getContainerPorts())
                .addAllToEnv(getCustomizedEnvs())
                .addNewEnv()
                .withName(ENV_FLINK_POD_IP_ADDRESS)
                .withValueFrom(
                        new EnvVarSourceBuilder()
                                .withNewFieldRef(API_VERSION, POD_IP_FIELD_PATH)
                                .build())
                .endEnv();
        getFlinkLogDirEnv().ifPresent(mainContainerBuilder::addToEnv);
        return mainContainerBuilder.build();
    }

    private List<ContainerPort> getContainerPorts() {
        if (kubernetesJobManagerParameters.isHostNetworkEnabled()) {
            return Collections.emptyList();
        }
        return Arrays.asList(
                new ContainerPortBuilder()
                        .withName(Constants.REST_PORT_NAME)
                        .withContainerPort(kubernetesJobManagerParameters.getRestPort())
                        .build(),
                new ContainerPortBuilder()
                        .withName(Constants.JOB_MANAGER_RPC_PORT_NAME)
                        .withContainerPort(kubernetesJobManagerParameters.getRPCPort())
                        .build(),
                new ContainerPortBuilder()
                        .withName(Constants.BLOB_SERVER_PORT_NAME)
                        .withContainerPort(kubernetesJobManagerParameters.getBlobServerPort())
                        .build());
    }

    private List<EnvVar> getCustomizedEnvs() {
        return kubernetesJobManagerParameters.getEnvironments().entrySet().stream()
                .map(
                        kv ->
                                new EnvVarBuilder()
                                        .withName(kv.getKey())
                                        .withValue(kv.getValue())
                                        .build())
                .collect(Collectors.toList());
    }

    private Optional<EnvVar> getFlinkLogDirEnv() {
        return kubernetesJobManagerParameters
                .getFlinkLogDirInPod()
                .map(logDir -> new EnvVar(Constants.ENV_FLINK_LOG_DIR, logDir, null));
    }
}
