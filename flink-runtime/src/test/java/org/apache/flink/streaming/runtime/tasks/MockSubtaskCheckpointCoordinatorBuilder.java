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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.taskmanager.AsyncExceptionHandler;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.BiFunctionWithException;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor.IMMEDIATE;
import static org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinatorImpl.openChannelStateWriter;

/** A mock builder to build {@link SubtaskCheckpointCoordinator}. */
public class MockSubtaskCheckpointCoordinatorBuilder {
    private String taskName = "mock-task";
    private CheckpointStorage checkpointStorage;
    private Environment environment;
    private AsyncExceptionHandler asyncExceptionHandler;
    private StreamTaskActionExecutor actionExecutor = IMMEDIATE;
    private ExecutorService executorService = Executors.newDirectExecutorService();
    private BiFunctionWithException<
                    ChannelStateWriter, Long, CompletableFuture<Void>, CheckpointException>
            prepareInputSnapshot = (channelStateWriter, aLong) -> FutureUtils.completedVoidFuture();
    private boolean unalignedCheckpointEnabled;
    private int maxSubtasksPerChannelStateFile = 5;
    private int maxRecordAbortedCheckpoints = 10;
    private boolean enableCheckpointAfterTasksFinished = true;

    public MockSubtaskCheckpointCoordinatorBuilder setEnvironment(Environment environment) {
        this.environment = environment;
        return this;
    }

    public MockSubtaskCheckpointCoordinatorBuilder setPrepareInputSnapshot(
            BiFunctionWithException<
                            ChannelStateWriter, Long, CompletableFuture<Void>, CheckpointException>
                    prepareInputSnapshot) {
        this.prepareInputSnapshot = prepareInputSnapshot;
        return this;
    }

    public MockSubtaskCheckpointCoordinatorBuilder setExecutor(ExecutorService executor) {
        this.executorService = executor;
        return this;
    }

    public MockSubtaskCheckpointCoordinatorBuilder setMaxRecordAbortedCheckpoints(
            int maxRecordAbortedCheckpoints) {
        this.maxRecordAbortedCheckpoints = maxRecordAbortedCheckpoints;
        return this;
    }

    public MockSubtaskCheckpointCoordinatorBuilder setUnalignedCheckpointEnabled(
            boolean unalignedCheckpointEnabled) {
        this.unalignedCheckpointEnabled = unalignedCheckpointEnabled;
        return this;
    }

    public MockSubtaskCheckpointCoordinatorBuilder setEnableCheckpointAfterTasksFinished(
            boolean enableCheckpointAfterTasksFinished) {
        this.enableCheckpointAfterTasksFinished = enableCheckpointAfterTasksFinished;
        return this;
    }

    SubtaskCheckpointCoordinator build() throws IOException {
        if (environment == null) {
            this.environment = MockEnvironment.builder().build();
        }
        if (checkpointStorage == null) {
            this.checkpointStorage = new JobManagerCheckpointStorage();
            this.environment.setCheckpointStorageAccess(
                    checkpointStorage.createCheckpointStorage(environment.getJobID()));
        }
        if (asyncExceptionHandler == null) {
            this.asyncExceptionHandler = new NonHandleAsyncException();
        }
        ChannelStateWriter channelStateWriter =
                unalignedCheckpointEnabled
                        ? openChannelStateWriter(
                                taskName,
                                () ->
                                        checkpointStorage.createCheckpointStorage(
                                                environment.getJobID()),
                                environment,
                                maxSubtasksPerChannelStateFile)
                        : ChannelStateWriter.NO_OP;

        return new SubtaskCheckpointCoordinatorImpl(
                checkpointStorage.createCheckpointStorage(environment.getJobID()),
                taskName,
                actionExecutor,
                executorService,
                environment,
                asyncExceptionHandler,
                prepareInputSnapshot,
                maxRecordAbortedCheckpoints,
                channelStateWriter,
                enableCheckpointAfterTasksFinished,
                (callable, duration) -> () -> {});
    }

    private static class NonHandleAsyncException implements AsyncExceptionHandler {

        @Override
        public void handleAsyncException(String message, Throwable exception) {
            // do nothing.
        }
    }
}
