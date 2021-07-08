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

import org.junit.Assume;
import org.junit.rules.ExternalResource;

/**
 * {@link ExternalResource} which has a configured real Kubernetes cluster and client. We assume
 * that one already has a running Kubernetes cluster. And all the ITCases assume that the
 * environment ITCASE_KUBECONFIG is set with a valid kube config file. In the E2E tests, we will use
 * a minikube for the testing.
 */
public class KubernetesResource extends ExternalResource {

    private static final String CLUSTER_ID = "flink-itcase-cluster";

    private static String kubeConfigFile;
    private Configuration configuration;
    private FlinkKubeClient flinkKubeClient;

    public static void checkEnv() {
        final String kubeConfigEnv = System.getenv("ITCASE_KUBECONFIG");
        Assume.assumeTrue(
                "ITCASE_KUBECONFIG environment is not set.",
                !StringUtils.isNullOrWhitespaceOnly(kubeConfigEnv));
        kubeConfigFile = kubeConfigEnv;
    }

    @Override
    public void before() {
        checkEnv();

        configuration = new Configuration();
        configuration.set(KubernetesConfigOptions.KUBE_CONFIG_FILE, kubeConfigFile);
        configuration.setString(KubernetesConfigOptions.CLUSTER_ID, CLUSTER_ID);
        final FlinkKubeClientFactory kubeClientFactory = new FlinkKubeClientFactory();
        flinkKubeClient = kubeClientFactory.fromConfiguration(configuration, "testing");
    }

    @Override
    public void after() {
        flinkKubeClient.close();
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public FlinkKubeClient getFlinkKubeClient() {
        return flinkKubeClient;
    }
}
