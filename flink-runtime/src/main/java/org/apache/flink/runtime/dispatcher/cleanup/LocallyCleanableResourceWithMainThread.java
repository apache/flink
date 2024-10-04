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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * {@code LocallyCleanableResourceWithMainThread} is an extension of the {@link
 * LocallyCleanableResource} interface. It allows the {@link DispatcherResourceCleanerFactory} to
 * inject the main thread as part of the cleanup procedure.
 *
 * <p>See {@code LocallyCleanableResource} for further context on the proper contract of the two
 * interfaces.
 *
 * @see org.apache.flink.runtime.dispatcher.cleanup.LocallyCleanableResource
 */
@FunctionalInterface
public interface LocallyCleanableResourceWithMainThread {

    /**
     * {@code localCleanupAsync} is expected to be called from the main thread and uses the passed
     * {@code mainThreadExecutor} for cleanups. Thread-safety must be ensured.
     *
     * @param jobId The {@link JobID} of the job for which the local data should be cleaned up.
     * @param cleanupExecutor The fallback executor for IO-heavy operations.
     * @param mainThreadExecutor The main thread executor.
     * @return The cleanup result future.
     */
    CompletableFuture<Void> localCleanupAsync(
            JobID jobId, Executor cleanupExecutor, ComponentMainThreadExecutor mainThreadExecutor);

    default LocallyCleanableResource toLocallyCleanableResource(
            ComponentMainThreadExecutor mainThreadExecutor) {
        return (jobId, cleanupExecutor) ->
                this.localCleanupAsync(jobId, cleanupExecutor, mainThreadExecutor);
    }
}
