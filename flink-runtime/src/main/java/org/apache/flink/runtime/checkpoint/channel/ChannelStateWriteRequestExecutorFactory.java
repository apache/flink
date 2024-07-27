/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/** The factory of {@link ChannelStateWriteRequestExecutor}. */
public class ChannelStateWriteRequestExecutorFactory {

    private final JobID jobID;

    private final Object lock = new Object();

    @GuardedBy("lock")
    private ChannelStateWriteRequestExecutor executor;

    public ChannelStateWriteRequestExecutorFactory(JobID jobID) {
        this.jobID = jobID;
    }

    public ChannelStateWriteRequestExecutor getOrCreateExecutor(
            JobVertexID jobVertexID,
            int subtaskIndex,
            SupplierWithException<CheckpointStorageWorkerView, ? extends IOException>
                    checkpointStorageWorkerViewSupplier,
            int maxSubtasksPerChannelStateFile) {
        return getOrCreateExecutor(
                jobVertexID,
                subtaskIndex,
                checkpointStorageWorkerViewSupplier,
                maxSubtasksPerChannelStateFile,
                true);
    }

    /**
     * @param startExecutor It is for test to prevent create too many threads when some unit tests
     *     create executor frequently.
     */
    ChannelStateWriteRequestExecutor getOrCreateExecutor(
            JobVertexID jobVertexID,
            int subtaskIndex,
            SupplierWithException<CheckpointStorageWorkerView, ? extends IOException>
                    checkpointStorageWorkerViewSupplier,
            int maxSubtasksPerChannelStateFile,
            boolean startExecutor) {
        synchronized (lock) {
            if (executor == null) {
                ChannelStateWriteRequestDispatcher dispatcher =
                        new ChannelStateWriteRequestDispatcherImpl(
                                checkpointStorageWorkerViewSupplier,
                                new ChannelStateSerializerImpl());
                executor =
                        new ChannelStateWriteRequestExecutorImpl(
                                dispatcher,
                                maxSubtasksPerChannelStateFile,
                                executor -> {
                                    assert Thread.holdsLock(lock);
                                    checkState(this.executor == executor);
                                    this.executor = null;
                                },
                                lock,
                                jobID);
                if (startExecutor) {
                    executor.start();
                }
            }
            ChannelStateWriteRequestExecutor currentExecutor = executor;
            currentExecutor.registerSubtask(jobVertexID, subtaskIndex);
            return currentExecutor;
        }
    }
}
