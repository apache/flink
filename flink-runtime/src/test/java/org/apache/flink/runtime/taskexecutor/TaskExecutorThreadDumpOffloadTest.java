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

import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.entrypoint.WorkingDirectory;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.rest.messages.ThreadDumpInfo;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcServiceExtension;
import org.apache.flink.runtime.taskmanager.LocalUnresolvedTaskManagerLocation;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that {@link TaskExecutor#requestThreadDump(Duration)} is offloaded to the ioExecutor. */
@ExtendWith(TestLoggerExtension.class)
class TaskExecutorThreadDumpOffloadTest {

    private static final Duration RPC_TIMEOUT = Duration.ofSeconds(10);
    private static final String IO_THREAD_NAME = "test-tm-io-thread";

    private final TestingRpcServiceExtension rpcServiceExtension = new TestingRpcServiceExtension();

    @RegisterExtension
    private final EachCallbackWrapper<TestingRpcServiceExtension> eachWrapper =
            new EachCallbackWrapper<>(rpcServiceExtension);

    @Test
    void requestThreadDumpRunsOnIoExecutor(@TempDir File tempDir) throws Exception {
        // Named single-thread executor: if the dump runs on it, dumpAllThreads() captures
        // the thread and it appears in the returned ThreadDumpInfo.
        final ExecutorService ioExecutor =
                Executors.newSingleThreadExecutor(r -> new Thread(r, IO_THREAD_NAME));
        try {
            final TaskManagerServices services =
                    new TaskManagerServicesBuilder()
                            .setUnresolvedTaskManagerLocation(
                                    new LocalUnresolvedTaskManagerLocation())
                            .setIoExecutor(ioExecutor)
                            .build();

            final TaskExecutor taskExecutor =
                    TaskExecutorBuilder.newBuilder(
                                    rpcServiceExtension.getTestingRpcService(),
                                    new TestingHighAvailabilityServicesBuilder().build(),
                                    WorkingDirectory.create(tempDir))
                            .setTaskManagerServices(services)
                            .build();
            try {
                taskExecutor.start();

                final ThreadDumpInfo dump =
                        taskExecutor
                                .getSelfGateway(TaskExecutorGateway.class)
                                .requestThreadDump(RPC_TIMEOUT)
                                .get(20, TimeUnit.SECONDS);

                assertThat(dump.getThreadInfos())
                        .as(
                                "dump must include ioExecutor thread '%s' (proves offload)",
                                IO_THREAD_NAME)
                        .anyMatch(t -> IO_THREAD_NAME.equals(t.getThreadName()));
            } finally {
                RpcUtils.terminateRpcEndpoint(taskExecutor);
            }
        } finally {
            ioExecutor.shutdownNow();
        }
    }
}
