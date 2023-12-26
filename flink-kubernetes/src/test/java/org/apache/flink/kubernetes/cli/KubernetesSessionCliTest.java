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

package org.apache.flink.kubernetes.cli;

import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.executors.KubernetesSessionClusterExecutor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/** Tests for the {@link KubernetesSessionCli}. */
class KubernetesSessionCliTest {

    @TempDir public Path confDirPath;

    @Test
    void testKubernetesSessionCliSetsDeploymentTargetCorrectly() throws CliArgsException {
        final KubernetesSessionCli cli =
                new KubernetesSessionCli(
                        new Configuration(), confDirPath.toAbsolutePath().toString());

        final String[] args = {};
        final Configuration configuration = cli.getEffectiveConfiguration(args);

        assertThat(KubernetesSessionClusterExecutor.NAME)
                .isEqualTo(configuration.get(DeploymentOptions.TARGET));
    }

    @Test
    void testDynamicProperties() throws Exception {

        final KubernetesSessionCli cli =
                new KubernetesSessionCli(
                        new Configuration(), confDirPath.toAbsolutePath().toString());
        final String[] args =
                new String[] {
                    "-e",
                    KubernetesSessionClusterExecutor.NAME,
                    "-Dpekko.ask.timeout=5 min",
                    "-Denv.java.opts=-DappName=foobar"
                };

        final Configuration executorConfig = cli.getEffectiveConfiguration(args);
        final ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);

        assertThat(clientFactory).isNotNull();

        final Map<String, String> executorConfigMap = executorConfig.toMap();
        assertThat(executorConfigMap).hasSize(4);
        assertThat(executorConfigMap)
                .contains(
                        entry("pekko.ask.timeout", "5 min"),
                        entry("env.java.opts", "-DappName=foobar"));
        assertThat(executorConfig.get(DeploymentOptionsInternal.CONF_DIR))
                .isEqualTo(confDirPath.toAbsolutePath().toString());
        assertThat(executorConfigMap).containsKey(DeploymentOptions.TARGET.key());
    }

    @Test
    void testCorrectSettingOfMaxSlots() throws Exception {
        final String[] params =
                new String[] {
                    "-e",
                    KubernetesSessionClusterExecutor.NAME,
                    "-D" + TaskManagerOptions.NUM_TASK_SLOTS.key() + "=3"
                };

        final KubernetesSessionCli cli = createFlinkKubernetesCustomCliWithJmAndTmTotalMemory(1234);

        final Configuration executorConfig = cli.getEffectiveConfiguration(params);
        final ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
        final ClusterSpecification clusterSpecification =
                clientFactory.getClusterSpecification(executorConfig);

        // each task manager has 3 slots but the parallelism is 7. Thus the slots should be
        // increased.
        assertThat(clusterSpecification.getSlotsPerTaskManager()).isEqualTo(3);
    }

    @Test
    void testResumeFromKubernetesID() throws Exception {
        final KubernetesSessionCli cli = createFlinkKubernetesCustomCliWithJmAndTmTotalMemory(1024);

        final String clusterId = "my-test-CLUSTER_ID";
        final String[] args =
                new String[] {
                    "-e",
                    KubernetesSessionClusterExecutor.NAME,
                    "-D" + KubernetesConfigOptions.CLUSTER_ID.key() + "=" + clusterId
                };

        final Configuration executorConfig = cli.getEffectiveConfiguration(args);
        final ClusterClientFactory clientFactory = getClusterClientFactory(executorConfig);

        assertThat(clientFactory.getClusterId(executorConfig)).isEqualTo(clusterId);
    }

    /**
     * Tests that the command line arguments override the configuration settings when the {@link
     * ClusterSpecification} is created.
     */
    @Test
    void testCommandLineClusterSpecification() throws Exception {
        final Configuration configuration = new Configuration();
        final int jobManagerMemory = 1337;
        final int taskManagerMemory = 7331;
        final int slotsPerTaskManager = 30;

        configuration.set(
                JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(jobManagerMemory));
        configuration.set(
                TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(taskManagerMemory));
        configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, slotsPerTaskManager);

        final String[] args = {
            "-e",
            KubernetesSessionClusterExecutor.NAME,
            "-D" + JobManagerOptions.TOTAL_PROCESS_MEMORY.key() + "=" + jobManagerMemory + "m",
            "-D" + TaskManagerOptions.TOTAL_PROCESS_MEMORY.key() + "=" + taskManagerMemory + "m",
            "-D" + TaskManagerOptions.NUM_TASK_SLOTS.key() + "=" + slotsPerTaskManager
        };

        final KubernetesSessionCli cli =
                new KubernetesSessionCli(configuration, confDirPath.toAbsolutePath().toString());

        Configuration executorConfig = cli.getEffectiveConfiguration(args);
        ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
        ClusterSpecification clusterSpecification =
                clientFactory.getClusterSpecification(executorConfig);

        assertThat(clusterSpecification.getMasterMemoryMB()).isEqualTo(jobManagerMemory);
        assertThat(clusterSpecification.getTaskManagerMemoryMB()).isEqualTo(taskManagerMemory);
        assertThat(clusterSpecification.getSlotsPerTaskManager()).isEqualTo(slotsPerTaskManager);
    }

    /**
     * Tests that the configuration settings are used to create the {@link ClusterSpecification}.
     */
    @Test
    void testConfigurationClusterSpecification() throws Exception {
        final Configuration configuration = new Configuration();
        final int jobManagerMemory = 1337;
        configuration.set(
                JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(jobManagerMemory));
        final int taskManagerMemory = 7331;
        configuration.set(
                TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(taskManagerMemory));
        final int slotsPerTaskManager = 42;
        configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, slotsPerTaskManager);

        final String[] args = {"-e", KubernetesSessionClusterExecutor.NAME};
        final KubernetesSessionCli cli =
                new KubernetesSessionCli(configuration, confDirPath.toAbsolutePath().toString());

        Configuration executorConfig = cli.getEffectiveConfiguration(args);
        ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
        ClusterSpecification clusterSpecification =
                clientFactory.getClusterSpecification(executorConfig);

        assertThat(clusterSpecification.getMasterMemoryMB()).isEqualTo(jobManagerMemory);
        assertThat(clusterSpecification.getTaskManagerMemoryMB()).isEqualTo(taskManagerMemory);
        assertThat(clusterSpecification.getSlotsPerTaskManager()).isEqualTo(slotsPerTaskManager);
    }

    /** Tests the specifying heap memory with unit (MB) for job manager and task manager. */
    @Test
    void testHeapMemoryPropertyWithUnitMB() throws Exception {
        final String[] args =
                new String[] {
                    "-e",
                    KubernetesSessionClusterExecutor.NAME,
                    "-D" + JobManagerOptions.TOTAL_PROCESS_MEMORY.key() + "=1024m",
                    "-D" + TaskManagerOptions.TOTAL_PROCESS_MEMORY.key() + "=2048m"
                };

        final KubernetesSessionCli cli = createFlinkKubernetesCustomCliWithJmAndTmTotalMemory(1024);

        final Configuration executorConfig = cli.getEffectiveConfiguration(args);
        final ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
        final ClusterSpecification clusterSpecification =
                clientFactory.getClusterSpecification(executorConfig);

        assertThat(clusterSpecification.getMasterMemoryMB()).isEqualTo(1024);
        assertThat(clusterSpecification.getTaskManagerMemoryMB()).isEqualTo(2048);
    }

    /** Tests the specifying heap memory with arbitrary unit for job manager and task manager. */
    @Test
    void testHeapMemoryPropertyWithArbitraryUnit() throws Exception {
        final String[] args =
                new String[] {
                    "-e",
                    KubernetesSessionClusterExecutor.NAME,
                    "-D" + JobManagerOptions.TOTAL_PROCESS_MEMORY.key() + "=1g",
                    "-D" + TaskManagerOptions.TOTAL_PROCESS_MEMORY.key() + "=3g"
                };

        final KubernetesSessionCli cli = createFlinkKubernetesCustomCliWithJmAndTmTotalMemory(1024);

        final Configuration executorConfig = cli.getEffectiveConfiguration(args);
        final ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
        final ClusterSpecification clusterSpecification =
                clientFactory.getClusterSpecification(executorConfig);

        assertThat(clusterSpecification.getMasterMemoryMB()).isEqualTo(1024);
        assertThat(clusterSpecification.getTaskManagerMemoryMB()).isEqualTo(3072);
    }

    /** Tests the specifying heap memory with old config key for job manager and task manager. */
    @Test
    void testHeapMemoryPropertyWithOldConfigKey() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(DeploymentOptions.TARGET, KubernetesSessionClusterExecutor.NAME);
        configuration.setInteger(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY_MB, 2048);
        configuration.setInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY_MB, 4096);

        final KubernetesSessionCli cli =
                new KubernetesSessionCli(configuration, confDirPath.toAbsolutePath().toString());

        final Configuration executorConfig = cli.getEffectiveConfiguration(new String[] {});
        final ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
        final ClusterSpecification clusterSpecification =
                clientFactory.getClusterSpecification(executorConfig);

        assertThat(clusterSpecification.getMasterMemoryMB()).isEqualTo(2048);
        assertThat(clusterSpecification.getTaskManagerMemoryMB()).isEqualTo(4096);
    }

    /**
     * Tests the specifying heap memory with config default value for job manager and task manager.
     */
    @Test
    void testHeapMemoryPropertyWithConfigDefaultValue() throws Exception {
        final String[] args = new String[] {"-e", KubernetesSessionClusterExecutor.NAME};

        final KubernetesSessionCli cli = createFlinkKubernetesCustomCliWithJmAndTmTotalMemory(1024);

        final Configuration executorConfig = cli.getEffectiveConfiguration(args);
        final ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
        final ClusterSpecification clusterSpecification =
                clientFactory.getClusterSpecification(executorConfig);

        assertThat(clusterSpecification.getMasterMemoryMB()).isEqualTo(1024);
        assertThat(clusterSpecification.getTaskManagerMemoryMB()).isEqualTo(1024);
    }

    private ClusterClientFactory<String> getClusterClientFactory(
            final Configuration executorConfig) {
        final ClusterClientServiceLoader clusterClientServiceLoader =
                new DefaultClusterClientServiceLoader();
        return clusterClientServiceLoader.getClusterClientFactory(executorConfig);
    }

    private KubernetesSessionCli createFlinkKubernetesCustomCliWithJmAndTmTotalMemory(
            int totalMemory) {
        Configuration configuration = new Configuration();
        configuration.set(
                JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(totalMemory));
        configuration.set(
                TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(totalMemory));
        return new KubernetesSessionCli(configuration, confDirPath.toAbsolutePath().toString());
    }
}
