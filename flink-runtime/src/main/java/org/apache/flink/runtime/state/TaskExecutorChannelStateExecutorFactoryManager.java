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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.Map;

/**
 * This class holds the all {@link ChannelStateWriteRequestExecutorFactory} objects for a task
 * executor (manager).
 */
@ThreadSafe
public class TaskExecutorChannelStateExecutorFactoryManager {

    /** Logger for this class. */
    private static final Logger LOG =
            LoggerFactory.getLogger(TaskExecutorChannelStateExecutorFactoryManager.class);

    private final Object lock = new Object();

    @GuardedBy("lock")
    private final Map<JobID, ChannelStateWriteRequestExecutorFactory> executorFactoryByJobId;

    @GuardedBy("lock")
    private boolean closed;

    public TaskExecutorChannelStateExecutorFactoryManager() {
        this.executorFactoryByJobId = new HashMap<>();
        this.closed = false;
    }

    public ChannelStateWriteRequestExecutorFactory getOrCreateExecutorFactory(
            @Nonnull JobID jobID) {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException(
                        "TaskExecutorChannelStateExecutorFactoryManager is already closed and cannot "
                                + "get a new executor factory.");
            }
            ChannelStateWriteRequestExecutorFactory factory = executorFactoryByJobId.get(jobID);
            if (factory == null) {
                LOG.info("Creating the channel state executor factory for job id {}", jobID);
                factory = new ChannelStateWriteRequestExecutorFactory(jobID);
                executorFactoryByJobId.put(jobID, factory);
            }
            return factory;
        }
    }

    public void releaseResourcesForJob(@Nonnull JobID jobID) {
        LOG.debug("Releasing the factory under job id {}", jobID);
        synchronized (lock) {
            if (closed) {
                return;
            }
            executorFactoryByJobId.remove(jobID);
        }
    }

    public void shutdown() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            executorFactoryByJobId.clear();
            LOG.info("Shutting down TaskExecutorChannelStateExecutorFactoryManager.");
        }
    }

    @VisibleForTesting
    @Nullable
    public ChannelStateWriteRequestExecutorFactory getFactoryByJobId(JobID jobId) {
        synchronized (lock) {
            return executorFactoryByJobId.get(jobId);
        }
    }
}
