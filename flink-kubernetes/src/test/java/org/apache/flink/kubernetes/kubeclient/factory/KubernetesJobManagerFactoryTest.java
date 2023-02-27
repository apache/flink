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

package org.apache.flink.kubernetes.kubeclient.factory;

import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypoint;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerSpecification;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.FlinkConfMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.HadoopConfMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.InternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.KerberosMountDecorator;
import org.apache.flink.kubernetes.kubeclient.services.HeadlessClusterIPService;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/** General tests for the {@link KubernetesJobManagerFactory}. */
class KubernetesJobManagerFactoryTest extends KubernetesJobManagerTestBase {

    private static final String SERVICE_ACCOUNT_NAME = "service-test";
    private static final String ENTRY_POINT_CLASS =
            KubernetesSessionClusterEntrypoint.class.getCanonicalName();

    private static final String EXISTING_HADOOP_CONF_CONFIG_MAP = "hadoop-conf";

    private static final String OWNER_REFERENCE_STRING =
            "apiVersion:cloudflow.io/v1beta1,blockOwnerDeletion:true,"
                    + "controller:true,kind:FlinkApplication,name:testapp,uid:e3c9aa3f-cc42-4178-814a-64aa15c82373";
    private static final List<OwnerReference> OWNER_REFERENCES =
            Collections.singletonList(
                    new OwnerReference(
                            "cloudflow.io/v1beta1",
                            true,
                            true,
                            "FlinkApplication",
                            "testapp",
                            "e3c9aa3f-cc42-4178-814a-64aa15c82373"));

    private static final int JOBMANAGER_REPLICAS = 2;

    private final FlinkPod flinkPod = new FlinkPod.Builder().build();

    protected KubernetesJobManagerSpecification kubernetesJobManagerSpecification;

    @Override
    protected void setupFlinkConfig() {
        super.setupFlinkConfig();

        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());
        flinkConfig.set(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS, ENTRY_POINT_CLASS);
        flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT, SERVICE_ACCOUNT_NAME);
        flinkConfig.set(
                SecurityOptions.KERBEROS_LOGIN_KEYTAB, kerberosDir.toString() + "/" + KEYTAB_FILE);
        flinkConfig.set(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, "test");
        flinkConfig.set(
                SecurityOptions.KERBEROS_KRB5_PATH, kerberosDir.toString() + "/" + KRB5_CONF_FILE);
        flinkConfig.setString(
                KubernetesConfigOptions.JOB_MANAGER_OWNER_REFERENCE.key(), OWNER_REFERENCE_STRING);
    }

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOGBACK_NAME);
        KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOG4J_NAME);

        generateKerberosFileItems();
    }

    @Test
    void testDeploymentMetadata() throws IOException {
        kubernetesJobManagerSpecification =
                KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                        flinkPod, kubernetesJobManagerParameters);
        final Deployment resultDeployment = this.kubernetesJobManagerSpecification.getDeployment();
        assertThat(resultDeployment.getApiVersion()).isEqualTo(Constants.APPS_API_VERSION);
        assertThat(resultDeployment.getMetadata().getName())
                .isEqualTo(KubernetesUtils.getDeploymentName(CLUSTER_ID));
        final Map<String, String> expectedLabels = getCommonLabels();
        expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
        expectedLabels.putAll(userLabels);
        assertThat(resultDeployment.getMetadata().getLabels()).isEqualTo(expectedLabels);

        assertThat(resultDeployment.getMetadata().getAnnotations()).isEqualTo(userAnnotations);

        assertThat(resultDeployment.getMetadata().getOwnerReferences())
                .contains(OWNER_REFERENCES.toArray(new OwnerReference[0]));
    }

    @Test
    void testDeploymentSpec() throws IOException {
        kubernetesJobManagerSpecification =
                KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                        flinkPod, kubernetesJobManagerParameters);

        final DeploymentSpec resultDeploymentSpec =
                this.kubernetesJobManagerSpecification.getDeployment().getSpec();
        assertThat(resultDeploymentSpec.getReplicas().intValue()).isEqualTo(1);

        final Map<String, String> expectedLabels = new HashMap<>(getCommonLabels());
        expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);

        assertThat(resultDeploymentSpec.getSelector().getMatchLabels()).isEqualTo(expectedLabels);

        expectedLabels.putAll(userLabels);
        assertThat(resultDeploymentSpec.getTemplate().getMetadata().getLabels())
                .isEqualTo(expectedLabels);

        assertThat(resultDeploymentSpec.getTemplate().getMetadata().getAnnotations())
                .isEqualTo(userAnnotations);

        assertThat(resultDeploymentSpec.getTemplate().getSpec()).isNotNull();
    }

    @Test
    void testPodSpec() throws IOException {
        kubernetesJobManagerSpecification =
                KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                        flinkPod, kubernetesJobManagerParameters);

        final PodSpec resultPodSpec =
                this.kubernetesJobManagerSpecification
                        .getDeployment()
                        .getSpec()
                        .getTemplate()
                        .getSpec();

        assertThat(resultPodSpec.getContainers()).hasSize(1);
        assertThat(resultPodSpec.getServiceAccountName()).isEqualTo(SERVICE_ACCOUNT_NAME);
        assertThat(resultPodSpec.getVolumes()).hasSize(3);

        final Container resultedMainContainer = resultPodSpec.getContainers().get(0);
        assertThat(resultedMainContainer.getName()).isEqualTo(Constants.MAIN_CONTAINER_NAME);
        assertThat(resultedMainContainer.getImage()).isEqualTo(CONTAINER_IMAGE);
        assertThat(resultedMainContainer.getImagePullPolicy())
                .isEqualTo(CONTAINER_IMAGE_PULL_POLICY.name());

        assertThat(resultedMainContainer.getEnv()).hasSize(3);
        assertThat(resultedMainContainer.getEnv().stream())
                .anyMatch(envVar -> envVar.getName().equals("key1"));

        assertThat(resultedMainContainer.getPorts()).hasSize(3);

        final Map<String, Quantity> requests = resultedMainContainer.getResources().getRequests();
        assertThat(requests.get("cpu").getAmount()).isEqualTo(Double.toString(JOB_MANAGER_CPU));
        assertThat(requests.get("memory").getAmount())
                .isEqualTo(String.valueOf(JOB_MANAGER_MEMORY));

        assertThat(resultedMainContainer.getCommand()).hasSize(1);
        // The args list is [bash, -c, 'java -classpath $FLINK_CLASSPATH ...'].
        assertThat(resultedMainContainer.getArgs()).hasSize(3);

        assertThat(resultedMainContainer.getVolumeMounts()).hasSize(3);
    }

    @Test
    void testAdditionalResourcesSize() throws IOException {
        kubernetesJobManagerSpecification =
                KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                        flinkPod, kubernetesJobManagerParameters);

        final List<HasMetadata> resultAdditionalResources =
                this.kubernetesJobManagerSpecification.getAccompanyingResources();
        assertThat(resultAdditionalResources).hasSize(5);

        final List<HasMetadata> resultServices =
                resultAdditionalResources.stream()
                        .filter(x -> x instanceof Service)
                        .collect(Collectors.toList());
        assertThat(resultServices).hasSize(2);

        final List<HasMetadata> resultConfigMaps =
                resultAdditionalResources.stream()
                        .filter(x -> x instanceof ConfigMap)
                        .collect(Collectors.toList());
        assertThat(resultConfigMaps).hasSize(2);

        final List<HasMetadata> resultSecrets =
                resultAdditionalResources.stream()
                        .filter(x -> x instanceof Secret)
                        .collect(Collectors.toList());
        assertThat(resultSecrets).hasSize(1);
    }

    @Test
    void testServices() throws IOException {
        kubernetesJobManagerSpecification =
                KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                        flinkPod, kubernetesJobManagerParameters);

        final List<Service> resultServices =
                this.kubernetesJobManagerSpecification.getAccompanyingResources().stream()
                        .filter(x -> x instanceof Service)
                        .map(x -> (Service) x)
                        .collect(Collectors.toList());

        assertThat(resultServices).hasSize(2);

        final List<Service> internalServiceCandidates =
                resultServices.stream()
                        .filter(
                                x ->
                                        x.getMetadata()
                                                .getName()
                                                .equals(
                                                        InternalServiceDecorator
                                                                .getInternalServiceName(
                                                                        CLUSTER_ID)))
                        .collect(Collectors.toList());
        assertThat(internalServiceCandidates).hasSize(1);

        final List<Service> restServiceCandidates =
                resultServices.stream()
                        .filter(
                                x ->
                                        x.getMetadata()
                                                .getName()
                                                .equals(
                                                        ExternalServiceDecorator
                                                                .getExternalServiceName(
                                                                        CLUSTER_ID)))
                        .collect(Collectors.toList());
        assertThat(restServiceCandidates).hasSize(1);

        final Service resultInternalService = internalServiceCandidates.get(0);
        assertThat(resultInternalService.getMetadata().getLabels()).hasSize(2);

        assertThat(resultInternalService.getSpec().getType()).isNull();
        assertThat(resultInternalService.getSpec().getClusterIP())
                .isEqualTo(HeadlessClusterIPService.HEADLESS_CLUSTER_IP);
        assertThat(resultInternalService.getSpec().getPorts()).hasSize(2);
        assertThat(resultInternalService.getSpec().getSelector()).hasSize(3);

        final Service resultRestService = restServiceCandidates.get(0);
        assertThat(resultRestService.getMetadata().getLabels()).hasSize(2);

        assertThat(resultRestService.getSpec().getType()).isEqualTo("ClusterIP");
        assertThat(resultRestService.getSpec().getPorts()).hasSize(1);
        assertThat(resultRestService.getSpec().getSelector()).hasSize(3);
    }

    @Test
    void testKerberosConfConfigMap() throws IOException {
        kubernetesJobManagerSpecification =
                KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                        flinkPod, kubernetesJobManagerParameters);

        final ConfigMap resultConfigMap =
                (ConfigMap)
                        this.kubernetesJobManagerSpecification.getAccompanyingResources().stream()
                                .filter(
                                        x ->
                                                x instanceof ConfigMap
                                                        && x.getMetadata()
                                                                .getName()
                                                                .equals(
                                                                        KerberosMountDecorator
                                                                                .getKerberosKrb5confConfigMapName(
                                                                                        CLUSTER_ID)))
                                .collect(Collectors.toList())
                                .get(0);

        assertThat(resultConfigMap.getApiVersion()).isEqualTo(Constants.API_VERSION);

        assertThat(resultConfigMap.getMetadata().getName())
                .isEqualTo(KerberosMountDecorator.getKerberosKrb5confConfigMapName(CLUSTER_ID));

        final Map<String, String> resultDatas = resultConfigMap.getData();
        assertThat(resultDatas).hasSize(1);
        assertThat(resultDatas.get(KRB5_CONF_FILE)).isEqualTo("some conf");
    }

    @Test
    void testKerberosKeytabSecret() throws IOException {
        kubernetesJobManagerSpecification =
                KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                        flinkPod, kubernetesJobManagerParameters);

        final Secret resultSecret =
                (Secret)
                        this.kubernetesJobManagerSpecification.getAccompanyingResources().stream()
                                .filter(
                                        x ->
                                                x instanceof Secret
                                                        && x.getMetadata()
                                                                .getName()
                                                                .equals(
                                                                        KerberosMountDecorator
                                                                                .getKerberosKeytabSecretName(
                                                                                        CLUSTER_ID)))
                                .collect(Collectors.toList())
                                .get(0);

        final Map<String, String> resultDatas = resultSecret.getData();
        assertThat(resultDatas).hasSize(1);
        assertThat(resultDatas.get(KEYTAB_FILE))
                .isEqualTo(Base64.getEncoder().encodeToString("some keytab".getBytes()));
    }

    @Test
    void testFlinkConfConfigMap() throws IOException {
        kubernetesJobManagerSpecification =
                KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                        flinkPod, kubernetesJobManagerParameters);

        final ConfigMap resultConfigMap =
                (ConfigMap)
                        getConfigMapList(
                                        FlinkConfMountDecorator.getFlinkConfConfigMapName(
                                                CLUSTER_ID))
                                .get(0);

        assertThat(resultConfigMap.getMetadata().getLabels()).hasSize(2);

        final Map<String, String> resultDatas = resultConfigMap.getData();
        assertThat(resultDatas).hasSize(3);
        assertThat(resultDatas.get(CONFIG_FILE_LOG4J_NAME)).isEqualTo("some data");
        assertThat(resultDatas.get(CONFIG_FILE_LOGBACK_NAME)).isEqualTo("some data");
        assertThat(resultDatas.get(FLINK_CONF_FILENAME))
                .contains(
                        KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS.key()
                                + ": "
                                + ENTRY_POINT_CLASS);
    }

    @Test
    void testExistingHadoopConfigMap() throws IOException {
        flinkConfig.set(
                KubernetesConfigOptions.HADOOP_CONF_CONFIG_MAP, EXISTING_HADOOP_CONF_CONFIG_MAP);
        kubernetesJobManagerSpecification =
                KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                        flinkPod, kubernetesJobManagerParameters);

        assertThat(kubernetesJobManagerSpecification.getAccompanyingResources())
                .noneMatch(
                        resource ->
                                resource.getMetadata()
                                        .getName()
                                        .equals(
                                                HadoopConfMountDecorator.getHadoopConfConfigMapName(
                                                        CLUSTER_ID)));

        final PodSpec podSpec =
                kubernetesJobManagerSpecification.getDeployment().getSpec().getTemplate().getSpec();
        assertThat(podSpec.getVolumes())
                .anyMatch(
                        volume ->
                                volume.getConfigMap()
                                        .getName()
                                        .equals(EXISTING_HADOOP_CONF_CONFIG_MAP));
    }

    @Test
    void testHadoopConfConfigMap() throws IOException {
        setHadoopConfDirEnv();
        generateHadoopConfFileItems();
        kubernetesJobManagerSpecification =
                KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                        flinkPod, kubernetesJobManagerParameters);

        final ConfigMap resultConfigMap =
                (ConfigMap)
                        getConfigMapList(
                                        HadoopConfMountDecorator.getHadoopConfConfigMapName(
                                                CLUSTER_ID))
                                .get(0);

        assertThat(resultConfigMap.getMetadata().getLabels()).hasSize(2);

        final Map<String, String> resultDatas = resultConfigMap.getData();
        assertThat(resultDatas).hasSize(2);
        assertThat(resultDatas.get("core-site.xml")).isEqualTo("some data");
        assertThat(resultDatas.get("hdfs-site.xml")).isEqualTo("some data");
    }

    @Test
    void testEmptyHadoopConfDirectory() throws IOException {
        setHadoopConfDirEnv();
        kubernetesJobManagerSpecification =
                KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                        flinkPod, kubernetesJobManagerParameters);

        assertThat(kubernetesJobManagerSpecification.getAccompanyingResources())
                .noneMatch(
                        resource ->
                                resource.getMetadata()
                                        .getName()
                                        .equals(
                                                HadoopConfMountDecorator.getHadoopConfConfigMapName(
                                                        CLUSTER_ID)));
    }

    @Test
    void testSetJobManagerDeploymentReplicas() throws Exception {
        flinkConfig.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.KUBERNETES.name());
        flinkConfig.set(
                KubernetesConfigOptions.KUBERNETES_JOBMANAGER_REPLICAS, JOBMANAGER_REPLICAS);
        kubernetesJobManagerSpecification =
                KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                        flinkPod, kubernetesJobManagerParameters);
        assertThat(kubernetesJobManagerSpecification.getDeployment().getSpec().getReplicas())
                .isEqualTo(JOBMANAGER_REPLICAS);
    }

    @Test
    void testHadoopDecoratorsCanBeTurnedOff() throws Exception {
        setHadoopConfDirEnv();
        generateHadoopConfFileItems();
        flinkConfig.set(
                KubernetesConfigOptions.KUBERNETES_HADOOP_CONF_MOUNT_DECORATOR_ENABLED, false);
        flinkConfig.set(KubernetesConfigOptions.KUBERNETES_KERBEROS_MOUNT_DECORATOR_ENABLED, false);
        kubernetesJobManagerSpecification =
                KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                        flinkPod, kubernetesJobManagerParameters);

        assertThat(
                        getConfigMapList(
                                HadoopConfMountDecorator.getHadoopConfConfigMapName(CLUSTER_ID)))
                .isEmpty();
        assertThat(
                        getConfigMapList(
                                KerberosMountDecorator.getKerberosKrb5confConfigMapName(
                                        CLUSTER_ID)))
                .isEmpty();
        assertThat(getConfigMapList(KerberosMountDecorator.getKerberosKeytabSecretName(CLUSTER_ID)))
                .isEmpty();
    }

    private List<HasMetadata> getConfigMapList(String configMapName) {
        return kubernetesJobManagerSpecification.getAccompanyingResources().stream()
                .filter(
                        x ->
                                x instanceof ConfigMap
                                        && x.getMetadata().getName().equals(configMapName))
                .collect(Collectors.toList());
    }
}
