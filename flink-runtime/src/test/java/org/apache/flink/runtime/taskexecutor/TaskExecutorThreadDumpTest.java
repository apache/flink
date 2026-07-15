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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.entrypoint.WorkingDirectory;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.rest.messages.ThreadDumpInfo;
import org.apache.flink.runtime.rest.messages.ThreadDumpMode;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcServiceExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link TaskExecutor#requestThreadDump(ThreadDumpMode, Duration)}: verifies that the RPC
 * returns a non-empty dump for each supported mode and honors the {@code
 * cluster.thread-dump.default-mode} fallback.
 */
@ExtendWith(TestLoggerExtension.class)
class TaskExecutorThreadDumpTest {

    private static final Duration RPC_TIMEOUT = Duration.ofSeconds(10);

    private final TestingRpcServiceExtension rpcServiceExtension = new TestingRpcServiceExtension();

    @RegisterExtension
    private final EachCallbackWrapper<TestingRpcServiceExtension> eachWrapper =
            new EachCallbackWrapper<>(rpcServiceExtension);

    @Test
    void requestThreadDumpReturnsNonEmptyDumpForEachMode(@TempDir File tempDir) throws Exception {
        for (ThreadDumpMode mode :
                new ThreadDumpMode[] {null, ThreadDumpMode.LITE, ThreadDumpMode.FULL}) {
            final ThreadDumpInfo dump = requestDump(tempDir, new Configuration(), mode);
            assertThat(dump.getThreadInfos()).isNotEmpty();
        }
    }

    @Test
    void requestThreadDumpFallsBackToClusterDefaultWhenModeOmitted(@TempDir File tempDir)
            throws Exception {
        // Override the cluster default to LITE so the assertion below distinguishes "config was
        // honored" from "hardcoded FULL". LITE omits the "Number of locked synchronizers" section
        // that FULL always emits.
        final Configuration configuration = new Configuration();
        configuration.set(ClusterOptions.THREAD_DUMP_DEFAULT_MODE, "LITE");

        final ThreadDumpInfo dump = requestDump(tempDir, configuration, /* mode= */ null);

        assertThat(dump.getThreadInfos())
                .noneMatch(
                        t ->
                                t.getStringifiedThreadInfo()
                                        .contains("Number of locked synchronizers"));
    }

    private ThreadDumpInfo requestDump(
            File tempDir, Configuration configuration, ThreadDumpMode mode) throws Exception {
        final TaskExecutor taskExecutor =
                TaskExecutorBuilder.newBuilder(
                                rpcServiceExtension.getTestingRpcService(),
                                new TestingHighAvailabilityServicesBuilder().build(),
                                WorkingDirectory.create(tempDir))
                        .setConfiguration(configuration)
                        .build();
        try {
            taskExecutor.start();
            return taskExecutor
                    .getSelfGateway(TaskExecutorGateway.class)
                    .requestThreadDump(mode, RPC_TIMEOUT)
                    .get(20, TimeUnit.SECONDS);
        } finally {
            RpcUtils.terminateRpcEndpoint(taskExecutor);
        }
    }
}
