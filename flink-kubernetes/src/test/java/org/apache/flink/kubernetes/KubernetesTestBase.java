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
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.util.TestLogger;

import io.fabric8.kubernetes.api.model.Config;
import io.fabric8.kubernetes.api.model.ConfigBuilder;
import io.fabric8.kubernetes.api.model.NamedClusterBuilder;
import io.fabric8.kubernetes.api.model.NamedContextBuilder;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Base test class for Kubernetes. */
public class KubernetesTestBase extends TestLogger {

    protected static final String NAMESPACE = "test";
    protected static final String CLUSTER_ID = "my-flink-cluster1";
    protected static final String CONTAINER_IMAGE = "flink-k8s-test:latest";
    protected static final String KEYTAB_FILE = "keytab";
    protected static final String KRB5_CONF_FILE = "krb5.conf";
    protected static final KubernetesConfigOptions.ImagePullPolicy CONTAINER_IMAGE_PULL_POLICY =
            KubernetesConfigOptions.ImagePullPolicy.IfNotPresent;
    protected static final int JOB_MANAGER_MEMORY = 768;

    @Rule public MixedKubernetesServer server = new MixedKubernetesServer(true, true);

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    protected File flinkConfDir;

    protected File hadoopConfDir;

    protected File kerberosDir;

    protected final Configuration flinkConfig = new Configuration();

    protected NamespacedKubernetesClient kubeClient;

    protected FlinkKubeClient flinkKubeClient;

    protected void setupFlinkConfig() {
        flinkConfig.setString(KubernetesConfigOptions.NAMESPACE, NAMESPACE);
        flinkConfig.setString(KubernetesConfigOptions.CLUSTER_ID, CLUSTER_ID);
        flinkConfig.setString(KubernetesConfigOptions.CONTAINER_IMAGE, CONTAINER_IMAGE);
        flinkConfig.set(
                KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY, CONTAINER_IMAGE_PULL_POLICY);
        flinkConfig.set(
                JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(JOB_MANAGER_MEMORY));
        flinkConfig.set(DeploymentOptionsInternal.CONF_DIR, flinkConfDir.toString());
    }

    protected void onSetup() throws Exception {}

    @Before
    public final void setup() throws Exception {
        flinkConfDir = temporaryFolder.newFolder().getAbsoluteFile();
        hadoopConfDir = temporaryFolder.newFolder().getAbsoluteFile();
        kerberosDir = temporaryFolder.newFolder().getAbsoluteFile();

        setupFlinkConfig();
        writeFlinkConfiguration();

        kubeClient = server.getClient().inNamespace(NAMESPACE);
        flinkKubeClient =
                new Fabric8FlinkKubeClient(
                        flinkConfig, kubeClient, Executors::newDirectExecutorService);

        onSetup();
    }

    @After
    public void tearDown() throws Exception {
        flinkKubeClient.close();
    }

    protected void writeFlinkConfiguration() throws IOException {
        BootstrapTools.writeConfiguration(
                this.flinkConfig, new File(flinkConfDir, "flink-conf.yaml"));
    }

    protected Map<String, String> getCommonLabels() {
        Map<String, String> labels = new HashMap<>();
        labels.put(Constants.LABEL_TYPE_KEY, Constants.LABEL_TYPE_NATIVE_TYPE);
        labels.put(Constants.LABEL_APP_KEY, CLUSTER_ID);
        return labels;
    }

    protected void setHadoopConfDirEnv() {
        Map<String, String> map = new HashMap<>();
        map.put(Constants.ENV_HADOOP_CONF_DIR, hadoopConfDir.toString());
        CommonTestUtils.setEnv(map, false);
    }

    protected void generateHadoopConfFileItems() throws IOException {
        KubernetesTestUtils.createTemporyFile("some data", hadoopConfDir, "core-site.xml");
        KubernetesTestUtils.createTemporyFile("some data", hadoopConfDir, "hdfs-site.xml");
    }

    protected void generateKerberosFileItems() throws IOException {
        KubernetesTestUtils.createTemporyFile("some keytab", kerberosDir, KEYTAB_FILE);
        KubernetesTestUtils.createTemporyFile("some conf", kerberosDir, KRB5_CONF_FILE);
    }

    protected String writeKubeConfigForMockKubernetesServer() throws Exception {
        final Config kubeConfig =
                new ConfigBuilder()
                        .withApiVersion(server.getClient().getApiVersion())
                        .withClusters(
                                new NamedClusterBuilder()
                                        .withName(CLUSTER_ID)
                                        .withNewCluster()
                                        .withNewServer(server.getClient().getMasterUrl().toString())
                                        .withInsecureSkipTlsVerify(true)
                                        .endCluster()
                                        .build())
                        .withContexts(
                                new NamedContextBuilder()
                                        .withName(CLUSTER_ID)
                                        .withNewContext()
                                        .withCluster(CLUSTER_ID)
                                        .withUser(
                                                server.getClient().getConfiguration().getUsername())
                                        .endContext()
                                        .build())
                        .withNewCurrentContext(CLUSTER_ID)
                        .build();
        final File kubeConfigFile = new File(temporaryFolder.newFolder(".kube"), "config");
        Serialization.yamlMapper().writeValue(kubeConfigFile, kubeConfig);
        return kubeConfigFile.getAbsolutePath();
    }
}
