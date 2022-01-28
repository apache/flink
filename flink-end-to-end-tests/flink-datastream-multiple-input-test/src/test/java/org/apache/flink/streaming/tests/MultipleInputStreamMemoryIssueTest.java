/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.util.config.memory.taskmanager.TaskExecutorFlinkMemory;
import org.apache.flink.runtime.util.config.memory.taskmanager.TaskExecutorFlinkMemoryUtils;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.JobSubmission;
import org.apache.flink.tests.util.flink.LocalStandaloneFlinkResourceFactory;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;

/** MultipleInputStreamTask Memory issue test. */
public class MultipleInputStreamMemoryIssueTest extends TestLogger {

    private static final Logger LOG =
            LoggerFactory.getLogger(MultipleInputStreamMemoryIssueTest.class);

    @Rule
    public final FlinkResource flink =
            new LocalStandaloneFlinkResourceFactory()
                    .create(
                            FlinkResourceSetup.builder()
                                    .addConfiguration(getConfiguration())
                                    .build());

    private ClusterController cluster;

    @BeforeEach
    public void init() throws Exception {
        cluster = flink.startCluster(1);
    }

    @AfterEach
    public void tearDown() throws Exception {
        cluster.close();
    }

    private static Configuration getConfiguration() {
        // we have to enable checkpoint to trigger flushing for filesystem sink
        final Configuration flinkConfig = new Configuration();
        /**
         * -Dtaskmanager.memory.jvm-overhead.fraction=0.1 \
         * -Dtaskmanager.memory.network.fraction=0.2 \ -Dtaskmanager.memory.managed.fraction=0.01 \
         */
        MemorySize processMem = MemorySize.ofMebiBytes(192);
        MemorySize metaspaceMem = MemorySize.ofMebiBytes(128);
        MemorySize overheadMem = MemorySize.ofMebiBytes(32);
        MemorySize flinkMem = processMem.subtract(metaspaceMem).subtract(overheadMem);
        MemorySize jvmHeap = flinkMem.multiply(0.4);
        MemorySize taskHeap = MemorySize.parse("12m");
        MemorySize frameworkHeap = jvmHeap.subtract(taskHeap);
        MemorySize offHeap = flinkMem.multiply(0.6);
        MemorySize managedMem = offHeap.multiply(0.5);
        MemorySize directMem = offHeap.multiply(0.5);
        MemorySize networkMem = MemorySize.parse("1024 bytes").multiply(250);
        MemorySize frameworkOffHeapMem = directMem.subtract(networkMem).multiply(.5);
        MemorySize taskOffHeapMem = directMem.subtract(networkMem).multiply(.5);

        flinkConfig.setString("taskmanager.memory.process.size", processMem.toString());
        flinkConfig.setString("taskmanager.memory.flink.size", flinkMem.toString());
        flinkConfig.setString("taskmanager.memory.task.heap.size", taskHeap.toString());
        flinkConfig.setString("taskmanager.memory.managed.size", managedMem.toString());
        flinkConfig.setString("taskmanager.memory.framework.heap.size", frameworkHeap.toString());
        flinkConfig.setString(
                "taskmanager.memory.framework.off-heap.size", frameworkOffHeapMem.toString());
        flinkConfig.setString("taskmanager.memory.task.off-heap.size", taskOffHeapMem.toString());
        flinkConfig.setString("taskmanager.memory.jvm-overhead.fraction", "0.1");
        flinkConfig.setString("taskmanager.memory.jvm-overhead.min", "16m");
        flinkConfig.setString("taskmanager.memory.jvm-metaspace.size", metaspaceMem.toString());
        flinkConfig.setString("taskmanager.memory.network.size", networkMem.toString());
        flinkConfig.setString("taskmanager.memory.network.min", "1 bytes");
        flinkConfig.setInteger("taskmanager.numberOfTaskSlots", 1);
        flinkConfig.setString("restart-strategy", "none");
        return flinkConfig;
    }

    @Test
    public void testMemoryConfiguration() {
        TaskExecutorFlinkMemoryUtils memoryUtils = new TaskExecutorFlinkMemoryUtils();
        TaskExecutorFlinkMemory memoryConfigs =
                memoryUtils.deriveFromRequiredFineGrainedOptions(getConfiguration());
        LOG.info(
                " flink memory: {}",
                memoryConfigs.getTotalFlinkMemorySize().toHumanReadableString());
        LOG.info(
                " jvm heap memory: {}",
                memoryConfigs.getJvmHeapMemorySize().toHumanReadableString());
        LOG.info(
                " jvm direct memory: {}",
                memoryConfigs.getJvmDirectMemorySize().toHumanReadableString());
        LOG.info(
                " framework heap memory: {}",
                memoryConfigs.getFrameworkHeap().toHumanReadableString());
        LOG.info(
                " framework off-heap memory: {}",
                memoryConfigs.getFrameworkOffHeap().toHumanReadableString());
        LOG.info(" task heap memory: {}", memoryConfigs.getTaskHeap().toHumanReadableString());
        LOG.info(
                " task off-heap memory: {}",
                memoryConfigs.getTaskOffHeap().toHumanReadableString());
        LOG.info(" flink managed memory: {}", memoryConfigs.getManaged().toHumanReadableString());
        LOG.info(" flink network memory: {}", memoryConfigs.getNetwork().toHumanReadableString());
    }

    @Test
    public void testMultipleInputStreamWithBroadcast() throws Exception {
        final Path jobJar = TestUtils.getResource("/jobs.jar");

        try (final ClusterController clusterController = flink.startCluster(1)) {
            // if the job fails then this throws an exception
            final Duration runFor = Duration.ofMinutes(1);
            Assertions.assertDoesNotThrow(
                    () -> {
                        clusterController.submitJob(
                                new JobSubmission.JobSubmissionBuilder(jobJar)
                                        .setDetached(false)
                                        .addArgument("--run-for", runFor.toString())
                                        .setMainClass(
                                                "org.apache.flink.streaming.tests.OneHighThroughputOneBroadcastJob")
                                        .build(),
                                runFor.plusSeconds(30));
                    },
                    "Job should not fail.");
        }
    }
}
