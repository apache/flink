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

package org.apache.flink.runtime.dispatcher.cleanup;

import org.apache.flink.api.common.JobID;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * {@code LocallyCleanableResource} is supposed to be implemented by any class that provides
 * artifacts for a given job that need to be cleaned up after the job reached a local terminal
 * state. Local cleanups that needs to be triggered for a global terminal state as well, need to be
 * implemented using the {@link GloballyCleanableResource}.
 *
 * <p>The {@link DispatcherResourceCleanerFactory} provides a workaround to trigger some {@code
 * LocallyCleanableResources} as globally cleanable. FLINK-26175 is created to cover a refactoring
 * and straighten things out.
 *
 * @see org.apache.flink.api.common.JobStatus
 */
@FunctionalInterface
public interface LocallyCleanableResource {

    /**
     * {@code localCleanupAsync} is expected to be called from the main thread. Heavy IO tasks
     * should be outsourced into the passed {@code cleanupExecutor}. Thread-safety must be ensured.
     *
     * @param jobId The {@link JobID} of the job for which the local data should be cleaned up.
     * @param cleanupExecutor The fallback executor for IO-heavy operations.
     * @return The cleanup result future.
     */
    CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor cleanupExecutor);
}
