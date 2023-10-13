/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.recovery;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.test.recovery.utils.TaskExecutorProcessEntryPoint;
import org.apache.flink.test.util.TestProcessBuilder;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Integration tests for the {@link TaskManagerRunner}. */
public class TaskManagerRunnerITCase extends TestLogger {

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Test
    public void testDeterministicWorkingDirIsNotDeletedInCaseOfProcessFailure() throws Exception {
        final File workingDirBase = TEMPORARY_FOLDER.newFolder();
        final ResourceID resourceId = ResourceID.generate();

        final Configuration configuration = new Configuration();
        configuration.set(
                ClusterOptions.PROCESS_WORKING_DIR_BASE, workingDirBase.getAbsolutePath());
        configuration.set(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID, resourceId.toString());
        configuration.set(JobManagerOptions.ADDRESS, "localhost");
        configuration.set(AkkaOptions.LOOKUP_TIMEOUT_DURATION, Duration.ZERO);

        final File workingDirectory =
                ClusterEntrypointUtils.generateTaskManagerWorkingDirectoryFile(
                        configuration, resourceId);

        final TestProcessBuilder.TestProcess taskManagerProcess =
                new TestProcessBuilder(TaskExecutorProcessEntryPoint.class.getName())
                        .addConfigAsMainClassArgs(configuration)
                        .start();

        boolean success = false;
        try {
            CommonTestUtils.waitUntilCondition(workingDirectory::exists);

            taskManagerProcess.getProcess().destroy();

            taskManagerProcess.getProcess().waitFor();

            assertTrue(workingDirectory.exists());
            success = true;
        } finally {
            if (!success) {
                AbstractTaskManagerProcessFailureRecoveryTest.printProcessLog(
                        "TaskManager", taskManagerProcess);
            }
        }
    }

    @Test
    public void testNondeterministicWorkingDirIsDeletedInCaseOfProcessFailure() throws Exception {
        final File workingDirBase = TEMPORARY_FOLDER.newFolder();

        final Configuration configuration = new Configuration();
        configuration.set(
                ClusterOptions.PROCESS_WORKING_DIR_BASE, workingDirBase.getAbsolutePath());
        configuration.set(JobManagerOptions.ADDRESS, "localhost");
        configuration.set(AkkaOptions.LOOKUP_TIMEOUT_DURATION, Duration.ZERO);

        final TestProcessBuilder.TestProcess taskManagerProcess =
                new TestProcessBuilder(TaskExecutorProcessEntryPoint.class.getName())
                        .addConfigAsMainClassArgs(configuration)
                        .start();

        boolean success = false;
        try {
            CommonTestUtils.waitUntilCondition(
                    () -> {
                        try (Stream<Path> files = Files.list(workingDirBase.toPath())) {
                            return files.findAny().isPresent();
                        }
                    });

            final File workingDirectory =
                    Iterables.getOnlyElement(
                                    Files.list(workingDirBase.toPath())
                                            .collect(Collectors.toList()))
                            .toFile();

            taskManagerProcess.getProcess().destroy();

            taskManagerProcess.getProcess().waitFor();

            assertFalse(workingDirectory.exists());
            success = true;
        } finally {
            if (!success) {
                AbstractTaskManagerProcessFailureRecoveryTest.printProcessLog(
                        "TaskManager", taskManagerProcess);
            }
        }
    }
}
