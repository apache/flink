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

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.DispatcherProcess;
import org.apache.flink.test.util.TestProcessBuilder;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for the {@link org.apache.flink.runtime.entrypoint.ClusterEntrypoint}. */
@ExtendWith(TestLoggerExtension.class)
class ClusterEntrypointITCase {

    @TempDir File tempFolder;

    @Test
    void testDeterministicWorkingDirectoryIsNotDeletedInCaseOfProcessFailure() throws Exception {
        final ResourceID resourceId = ResourceID.generate();

        final Configuration configuration = new Configuration();
        configuration.set(ClusterOptions.PROCESS_WORKING_DIR_BASE, tempFolder.getAbsolutePath());
        configuration.set(JobManagerOptions.JOB_MANAGER_RESOURCE_ID, resourceId.toString());

        final File workingDirectory =
                ClusterEntrypointUtils.generateJobManagerWorkingDirectoryFile(
                        configuration, resourceId);

        final TestProcessBuilder.TestProcess jobManagerProcess =
                new TestProcessBuilder(
                                DispatcherProcess.DispatcherProcessEntryPoint.class.getName())
                        .addConfigAsMainClassArgs(configuration)
                        .start();

        boolean success = false;
        try {
            CommonTestUtils.waitUntilCondition(workingDirectory::exists);

            jobManagerProcess.getProcess().destroy();

            jobManagerProcess.getProcess().waitFor();

            assertThat(workingDirectory).exists();
            success = true;
        } finally {
            if (!success) {
                AbstractTaskManagerProcessFailureRecoveryTest.printProcessLog(
                        "JobManager", jobManagerProcess);
            }
        }
    }

    @Test
    void testNondeterministicWorkingDirectoryIsDeletedInCaseOfProcessFailure() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(ClusterOptions.PROCESS_WORKING_DIR_BASE, tempFolder.getAbsolutePath());

        final TestProcessBuilder.TestProcess jobManagerProcess =
                new TestProcessBuilder(
                                DispatcherProcess.DispatcherProcessEntryPoint.class.getName())
                        .addConfigAsMainClassArgs(configuration)
                        .start();

        boolean success = false;
        try {
            CommonTestUtils.waitUntilCondition(
                    () -> {
                        try (Stream<Path> files = Files.list(tempFolder.toPath())) {
                            return files.findAny().isPresent();
                        }
                    });

            final File workingDirectory;
            try (Stream<Path> files = Files.list(tempFolder.toPath())) {
                workingDirectory = files.findFirst().orElseThrow().toFile();
            }

            jobManagerProcess.getProcess().destroy();

            jobManagerProcess.getProcess().waitFor();

            assertThat(workingDirectory).doesNotExist();
            success = true;
        } finally {
            if (!success) {
                AbstractTaskManagerProcessFailureRecoveryTest.printProcessLog(
                        "JobManager", jobManagerProcess);
            }
        }
    }
}
