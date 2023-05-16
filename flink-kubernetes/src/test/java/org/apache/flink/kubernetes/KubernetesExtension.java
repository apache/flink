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

package org.apache.flink.kubernetes;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.util.StringUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.rules.ExternalResource;

/**
 * {@link ExternalResource} which has a configured real Kubernetes cluster and client. We assume
 * that one already has a running Kubernetes cluster. And all the ITCases assume that the
 * environment ITCASE_KUBECONFIG is set with a valid kube config file. In the E2E tests, we will use
 * a minikube for the testing.
 *
 * <p>If a test depends on this extension, please annotation it with {@code @EnabledIf(value =
 * "org.apache.flink.kubernetes.KubernetesExtension#checkEnv", disabledReason = "Disabled as " +
 * KubernetesExtension.KUBE_CONF_ENV + " is not set.")} to avoid failure in the environment without
 * Kubernetes cluster.
 */
public class KubernetesExtension implements BeforeAllCallback, AfterAllCallback {

    private static final String CLUSTER_ID = "flink-itcase-cluster";

    public static final String KUBE_CONF_ENV = "ITCASE_KUBECONFIG";

    private static final int KUBERNETES_TRANSACTIONAL_OPERATION_MAX_RETRIES = 100;

    private static String kubeConfigFile;
    private Configuration configuration;
    private FlinkKubeClient flinkKubeClient;

    public static boolean checkEnv() {
        final String kubeConfigEnv = System.getenv(KUBE_CONF_ENV);
        return !StringUtils.isNullOrWhitespaceOnly(kubeConfigEnv);
    }

    private void checkAndSetKubeConfigFile() {
        Assertions.assertThat(checkEnv())
                .withFailMessage(
                        "This extension can be used only when "
                                + KUBE_CONF_ENV
                                + " environment is set.")
                .isTrue();
        kubeConfigFile = System.getenv(KUBE_CONF_ENV);
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        checkAndSetKubeConfigFile();
        configuration = new Configuration();
        configuration.set(KubernetesConfigOptions.KUBE_CONFIG_FILE, kubeConfigFile);
        configuration.setString(KubernetesConfigOptions.CLUSTER_ID, CLUSTER_ID);
        configuration.set(
                KubernetesConfigOptions.KUBERNETES_TRANSACTIONAL_OPERATION_MAX_RETRIES,
                KUBERNETES_TRANSACTIONAL_OPERATION_MAX_RETRIES);
        final FlinkKubeClientFactory kubeClientFactory = new FlinkKubeClientFactory();
        flinkKubeClient = kubeClientFactory.fromConfiguration(configuration, "testing");
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        if (flinkKubeClient != null) {
            flinkKubeClient.close();
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public FlinkKubeClient getFlinkKubeClient() {
        return flinkKubeClient;
    }
}
