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

import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesPodTestBase;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParametersTest;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** General tests for the {@link KerberosMountDecoratorTest}. */
class KerberosMountDecoratorTest extends KubernetesPodTestBase {

    private KerberosMountDecorator kerberosMountDecorator;

    private AbstractKubernetesParametersTest.TestingKubernetesParameters
            testingKubernetesParameters;

    private static final String CUSTOM_KRB5_CONF_FILE = "mykrb5.conf";

    @Override
    protected void setupFlinkConfig() {
        super.setupFlinkConfig();

        flinkConfig.set(
                SecurityOptions.KERBEROS_LOGIN_KEYTAB, kerberosDir.toString() + "/" + KEYTAB_FILE);
        flinkConfig.set(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, "test");
        flinkConfig.set(
                SecurityOptions.KERBEROS_KRB5_PATH,
                kerberosDir.toString() + "/" + CUSTOM_KRB5_CONF_FILE);
    }

    @Override
    protected void generateKerberosFileItems() throws IOException {
        KubernetesTestUtils.createTemporyFile("some keytab", kerberosDir, KEYTAB_FILE);
        KubernetesTestUtils.createTemporyFile("some conf", kerberosDir, CUSTOM_KRB5_CONF_FILE);
    }

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        generateKerberosFileItems();

        this.testingKubernetesParameters =
                new AbstractKubernetesParametersTest.TestingKubernetesParameters(flinkConfig);
        this.kerberosMountDecorator = new KerberosMountDecorator(testingKubernetesParameters);
    }

    @Test
    void testWhetherPodOrContainerIsDecorated() {
        final FlinkPod resultFlinkPod = kerberosMountDecorator.decorateFlinkPod(baseFlinkPod);
        assertThat(resultFlinkPod.getPodWithoutMainContainer())
                .isNotEqualTo(baseFlinkPod.getPodWithoutMainContainer());
        assertThat(resultFlinkPod.getMainContainer()).isNotEqualTo(baseFlinkPod.getMainContainer());
    }

    @Test
    void testConfEditWhenBuildAccompanyingKubernetesResources() throws IOException {
        kerberosMountDecorator.buildAccompanyingKubernetesResources();

        assertThat(
                        this.testingKubernetesParameters
                                .getFlinkConfiguration()
                                .get(SecurityOptions.KERBEROS_LOGIN_KEYTAB))
                .isEqualTo(
                        String.format("%s/%s", Constants.KERBEROS_KEYTAB_MOUNT_POINT, KEYTAB_FILE));
    }

    @Test
    void testDecoratedFlinkContainer() {
        final Container resultMainContainer =
                kerberosMountDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();
        assertThat(resultMainContainer.getVolumeMounts()).hasSize(2);

        final VolumeMount keytabVolumeMount =
                resultMainContainer.getVolumeMounts().stream()
                        .filter(x -> x.getName().equals(Constants.KERBEROS_KEYTAB_VOLUME))
                        .collect(Collectors.toList())
                        .get(0);
        final VolumeMount krb5ConfVolumeMount =
                resultMainContainer.getVolumeMounts().stream()
                        .filter(x -> x.getName().equals(Constants.KERBEROS_KRB5CONF_VOLUME))
                        .collect(Collectors.toList())
                        .get(0);
        assertThat(keytabVolumeMount).isNotNull();
        assertThat(krb5ConfVolumeMount).isNotNull();
        assertThat(keytabVolumeMount.getMountPath())
                .isEqualTo(Constants.KERBEROS_KEYTAB_MOUNT_POINT);
        assertThat(krb5ConfVolumeMount.getMountPath())
                .isEqualTo(Constants.KERBEROS_KRB5CONF_MOUNT_DIR + "/krb5.conf");
    }

    @Test
    void testDecoratedFlinkPodVolumes() {
        final FlinkPod resultFlinkPod = kerberosMountDecorator.decorateFlinkPod(baseFlinkPod);
        List<Volume> volumes = resultFlinkPod.getPodWithoutMainContainer().getSpec().getVolumes();
        assertThat(volumes).hasSize(2);

        final Volume keytabVolume =
                volumes.stream()
                        .filter(x -> x.getName().equals(Constants.KERBEROS_KEYTAB_VOLUME))
                        .collect(Collectors.toList())
                        .get(0);
        final Volume krb5ConfVolume =
                volumes.stream()
                        .filter(x -> x.getName().equals(Constants.KERBEROS_KRB5CONF_VOLUME))
                        .collect(Collectors.toList())
                        .get(0);
        assertThat(keytabVolume.getSecret()).isNotNull();
        assertThat(keytabVolume.getSecret().getSecretName())
                .isEqualTo(
                        KerberosMountDecorator.getKerberosKeytabSecretName(
                                testingKubernetesParameters.getClusterId()));

        assertThat(krb5ConfVolume.getConfigMap()).isNotNull();
        assertThat(krb5ConfVolume.getConfigMap().getName())
                .isEqualTo(
                        KerberosMountDecorator.getKerberosKrb5confConfigMapName(
                                testingKubernetesParameters.getClusterId()));
        assertThat(krb5ConfVolume.getConfigMap().getItems()).hasSize(1);
        assertThat(krb5ConfVolume.getConfigMap().getItems().get(0).getKey())
                .isEqualTo(CUSTOM_KRB5_CONF_FILE);
        assertThat(krb5ConfVolume.getConfigMap().getItems().get(0).getPath())
                .isEqualTo(KRB5_CONF_FILE);
    }
}
