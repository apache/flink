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

import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesTaskManagerTestBase;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/** General tests for the {@link KubernetesTaskManagerFactory}. */
class KubernetesTaskManagerFactoryTest extends KubernetesTaskManagerTestBase {

    private Pod resultPod;

    @Override
    protected void setupFlinkConfig() {
        super.setupFlinkConfig();

        flinkConfig.set(
                SecurityOptions.KERBEROS_LOGIN_KEYTAB, kerberosDir.toString() + "/" + KEYTAB_FILE);
        flinkConfig.set(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, "test");
        flinkConfig.set(
                SecurityOptions.KERBEROS_KRB5_PATH, kerberosDir.toString() + "/" + KRB5_CONF_FILE);
    }

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOGBACK_NAME);
        KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOG4J_NAME);

        setHadoopConfDirEnv();
        generateHadoopConfFileItems();

        generateKerberosFileItems();

        this.resultPod =
                KubernetesTaskManagerFactory.buildTaskManagerKubernetesPod(
                                new FlinkPod.Builder().build(), kubernetesTaskManagerParameters)
                        .getInternalResource();
    }

    @Test
    void testPod() {
        assertThat(this.resultPod.getMetadata().getName()).isEqualTo(POD_NAME);
        assertThat(this.resultPod.getMetadata().getLabels()).hasSize(5);
        assertThat(this.resultPod.getSpec().getVolumes()).hasSize(4);
    }

    @Test
    void testContainer() {
        final List<Container> resultContainers = this.resultPod.getSpec().getContainers();
        assertThat(resultContainers).hasSize(1);

        final Container resultMainContainer = resultContainers.get(0);
        assertThat(resultMainContainer.getName()).isEqualTo(Constants.MAIN_CONTAINER_NAME);
        assertThat(resultMainContainer.getImage()).isEqualTo(CONTAINER_IMAGE);
        assertThat(resultMainContainer.getImagePullPolicy())
                .isEqualTo(CONTAINER_IMAGE_PULL_POLICY.name());

        assertThat(resultMainContainer.getEnv()).hasSize(5);
        assertThat(
                        resultMainContainer.getEnv().stream()
                                .anyMatch(envVar -> envVar.getName().equals("key1")))
                .isTrue();

        assertThat(resultMainContainer.getPorts()).hasSize(1);
        assertThat(resultMainContainer.getCommand()).hasSize(1);
        // The args list is [bash, -c, 'java -classpath $FLINK_CLASSPATH ...'].
        assertThat(resultMainContainer.getArgs()).hasSize(3);
        assertThat(resultMainContainer.getVolumeMounts()).hasSize(4);
    }

    @Test
    void testHadoopDecoratorsCanBeTurnedOff() {
        flinkConfig.set(
                KubernetesConfigOptions.KUBERNETES_HADOOP_CONF_MOUNT_DECORATOR_ENABLED, false);
        flinkConfig.set(KubernetesConfigOptions.KUBERNETES_KERBEROS_MOUNT_DECORATOR_ENABLED, false);

        Pod pod =
                KubernetesTaskManagerFactory.buildTaskManagerKubernetesPod(
                                new FlinkPod.Builder().build(), kubernetesTaskManagerParameters)
                        .getInternalResource();
        assertThat(pod.getSpec().getVolumes()).hasSize(1);
    }
}
